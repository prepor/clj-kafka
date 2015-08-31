(ns ru.prepor.clj-kafka.elastic
  (:require [clojure.core.async :as a]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [ru.prepor.clj-kafka :as core]
            [ru.prepor.clj-kafka.tracer :as trace])
  (:import [org.apache.curator.framework.recipes.cache PathChildrenCache
            PathChildrenCacheListener PathChildrenCache$StartMode PathChildrenCacheEvent$Type]
           [org.apache.zookeeper CreateMode]
           [org.apache.zookeeper KeeperException$NodeExistsException]))

;; Реализация распределенного master-less консьюмера кафки.
;; - Для координации  использует зукипер.
;; - Автоматически балансирует партишиены в рамках одной группы на всех консьюмеры
;; которые подключены как члены группы.
;; - Ключевым элементом API является core.async
;;
;; Способ балансировки косньюмера:
;; - Объявляет себя в списке консюмеров (эфемерная секвеншиал нода)
;; - Подписывается на на все изменения списка консьюмеров
;; - Получает весь список партишенов топиков
;; - Искходя из числа консьюмеров и своего положения в списке консьюмеров
;; определяет какие именно партишены должен консьюмить он сам (тупо
;; consumers-count % parition-id == my-index)
;; - Перед стартом консьюминга ставится лок в зукипере на эту группу+партишен
;; - Если лок не удается поставить, то ждем пока не удасться (или не произойдет перебалансировки)
;; - После перебалансировки и решения о том, что партишен нам больше не
;; принадлежит ожидаем ack для последнего сообщения, которое было отправлено
;; пользователю библиотеки, после чего удаляем лок в зукипере

(defn zookeeper-path
  [curator path & [{:keys [start-mode cache-data] :or {cache-data false}}]]
  (let [start-mode* (case start-mode
                      :normal PathChildrenCache$StartMode/NORMAL
                      :post-initialized-event
                      PathChildrenCache$StartMode/POST_INITIALIZED_EVENT)
        children-cache (PathChildrenCache. curator path cache-data)
        out (a/chan)]
    (-> (.getListenable children-cache)
        (.addListener (reify PathChildrenCacheListener
                        (childEvent [this client event]
                          (log/debug "Zookeeper path event" path event)
                          (condp = (.getType event)
                            PathChildrenCacheEvent$Type/CHILD_ADDED
                            (a/>!! out {:type :child-added
                                        :path (-> event .getData .getPath)})
                            PathChildrenCacheEvent$Type/CHILD_REMOVED
                            (a/>!! out {:type :child-removed
                                        :path (-> event .getData .getPath)})
                            PathChildrenCacheEvent$Type/CONNECTION_LOST
                            (a/close! out)
                            PathChildrenCacheEvent$Type/INITIALIZED
                            (a/>!! out {:type :initialized})
                            nil)))))
    (.start children-cache start-mode*)
    [(fn [] (.close children-cache) (a/close! out)) out]))

(defn initialized-zookeeper-path
  [curator path & [params]]
  (let [[stop-fn changes] (zookeeper-path curator path
                                          (assoc params
                                                 :start-mode :post-initialized-event))]
    (a/go
      (let [init (loop [acc []]
                   (when-let [v (a/<! changes)]
                     (if (= :initialized (:type v))
                       (do
                         (log/debug "Initialized zookeeper path" path acc)
                         acc)
                       (recur (conj acc (:path v))))))]
        [stop-fn init changes]))))

(defn consumers-accumulator
  [initial zk-consumers-ch]
  (let [out (a/chan)
        initial' (set initial)]
    (a/go
      (a/>! out initial')
      (loop [consumers initial']
        (if-let [change (a/<! zk-consumers-ch)]
          (let [consumers (case (:type change)
                            :child-added (conj consumers (:path change))
                            :child-removed (disj consumers (:path change)))]
            (a/>! out consumers)
            (recur consumers))
          (a/close! out))))
    out))

(defn my-partitions
  [consumers me sorted-partitions]
  (when (seq consumers)
    (let [n (.indexOf (vec (sort consumers)) me)
          total (count consumers)]
      (->>
       (for [[i v] (map-indexed vector sorted-partitions)
             :when (= n (mod i total))]
         v)
       (into #{})))))

(defn diff
  [old new]
  {:added (seq (set/difference new old))
   :removed (seq (set/difference old new))})

(defn partitions-solver
  [consumers-ch me partitions]
  (let [out (a/chan)
        sorted-partitions (sort-by (fn [v] [(:topic v) (:id v)])  partitions)]
    (a/go-loop [current-partitions #{}]
      (if-let [consumers (a/<! consumers-ch)]
        (let [current-partitions' (my-partitions consumers me sorted-partitions)
              {:keys [added removed]} (diff current-partitions current-partitions')]
          (doseq [p removed]
            (a/>! out {:type :remove :partition p}))
          (doseq [p added]
            (a/>! out {:type :add :partition p}))
          (recur current-partitions'))
        (a/close! out)))
    out))

;; Получет сообщение из buffered-messages-ch (кафка) и отправляет в
;; messages-ch (канал, который читает клиентский код). В acks-ch складываются полученные аки, от control-ch всегда стоит ожидать закрытия. В таком случае мы дожидаемся ака из acks-ch соответсвующего последнему отправленному сообщению и закрываем канал
(defn partition-worker
  [control-ch acks-ch buffered-messages-ch messages-ch {:keys [message-clb registry-clb]}]
  (a/go-loop [state {:last-ack 0 :last-offset nil :completing? false}]
    ;; только для мониторинга
    (registry-clb state)
    (if (:completing? state)
      (when (not (or (nil? (:last-offset state)) (= (:last-ack state) (:last-offset state))))
        (recur (assoc state :last-ack (a/<! acks-ch))))
      (let [ports (-> [control-ch acks-ch]
                      (conj (if-let [m (:current-message state)]
                              [messages-ch m]
                              buffered-messages-ch)))
            ;; С приоритетом тут потому, что нам бы неплохо сначала читать
            ;; из control-ch а потом уже сообщения. В целом без приоритета
            ;; ничего плохого не произойдет, но мы может продолжить читать
            ;; все сообщения из буфера вместо того, что бы просто
            ;; остановиться
            [val port] (a/alts! ports :priority true)]
        (condp = port
          control-ch (recur (assoc state :completing? true))
          acks-ch (recur (assoc state :last-ack val))
          messages-ch (let [offset (:offset (:current-message state))]
                        ;; только для мониторинга
                        (message-clb offset)
                        (recur (assoc state :last-offset offset :current-message nil)))
          buffered-messages-ch  (recur (assoc state :current-message val)))))))

(defn start-partition*
  [kafka channels group init-offsets {:keys [topic id]}]
  (let [messages-ch (a/chan)
        control-ch (a/chan)
        acks-ch (a/chan)
        ack-clb (fn [message] (a/go (a/>! acks-ch (:offset message))))
        message-clb #(trace/message-received (:tracer kafka) group topic id %)
        registry-clb #(let [data-only (update-in % [:current-message] dissoc :kafka :ack-clb)]
                        (swap! (:worker-registry kafka) assoc [group topic id] data-only))
        partition-consumer-params {:topic topic
                                  :partition id
                                  :init-offsets init-offsets
                                  :control-ch control-ch
                                  :ack-clb ack-clb
                                  :buf-or-n 50}
        worker-go (a/go
                    (let [[init-offset buffered-messages-ch]
                          (a/<! (core/partition-consumer kafka partition-consumer-params))
                          partition-data {:topic topic
                                          :partition id
                                          :init-offset init-offset
                                          :chan messages-ch}]
                      (when (= :continue (a/alt!
                                           control-ch ([_] :stop)
                                           [[channels partition-data]] :continue))
                        (a/<! (partition-worker control-ch acks-ch buffered-messages-ch messages-ch
                                                {:message-clb message-clb
                                                 :registry-clb registry-clb}))
                        (a/close! acks-ch)
                        (a/close! messages-ch)
                        (swap! (:worker-registry kafka) dissoc [group topic id]))))]
    (fn [] (a/close! control-ch) worker-go)))

(defn partition-path
  [group topic id]
  (format "/partitions/%s/%s/%s" group topic id))

;; Пытаемся залочить партишен, но не забываем смотреть на контрольный канал, в
;; котором нам могут приказать перестать это делать.
(defn start-partition
  [kafka channels group init-offsets {:keys [topic id] :as partition}]
  (let [path (partition-path group topic id)
        stop-ch (a/chan)
        stopper #(a/close! stop-ch)
        tick (fn [] (try
                     (->  @(:curator kafka)
                          .create
                          (.creatingParentsIfNeeded)
                          (.withMode CreateMode/EPHEMERAL)
                          (.forPath path (.getBytes (:host kafka))))
                     (start-partition* kafka channels group init-offsets partition)
                     (catch KeeperException$NodeExistsException e
                       (log/warn "Zookeeper path already exists" path)
                       nil)))
        worker (a/go-loop []
                 (if-let [stopper' (tick)]
                   (do (a/<! stop-ch)
                       (a/<! (stopper'))
                       (-> @(:curator kafka) .delete (.forPath path)))
                   (a/alt!
                     stop-ch ([_] nil)
                     (a/timeout 100) (recur))))]
    (fn [] (a/close! stop-ch) worker)))

;; Старт нового консьюмера происходит асинхронно, остановка - синхронно. Это
;; важно, т.к. остановить консьюмер мы можем всегда, а вот стартовать не
;; обязательно. Если какой-то другой консьюмер решил, что ему нужен этот
;; партишен, а мы решим это только следующим шагом, то зависнем здесь навсегда
(defn partitions-supervisor
  [kafka group init-offsets partitions-changes-ch]
  (let [channels (a/chan)]
    (a/go-loop [partition-stoppers {}]
      (if-let [command (a/<! partitions-changes-ch)]
        (case (:type command)
          :add (let [stopper (start-partition kafka channels group init-offsets (:partition command))]
                 (recur (assoc partition-stoppers (:partition command) stopper)))
          :remove (let [partition-stopper (partition-stoppers (:partition command))]
                    (a/<! (partition-stopper))
                    (recur (dissoc partition-stoppers (:partition command)))))
        (do (doseq [partition-stopper (vals partition-stoppers)]
              (a/<! (partition-stopper)))
            (a/close! channels))))
    channels))

(defn all-partitions
  [kafka topics]
  (for [t (core/topics-metadata kafka topics)
        partition (:partition-metadata t)]
    {:topic (:topic t) :id (:id partition)}))

(defn introduce-myself
  [kafka group]
  (-> @(:curator kafka)
      .create
      (.creatingParentsIfNeeded)
      (.withMode CreateMode/EPHEMERAL_SEQUENTIAL)
      (.forPath (format "/consumers/%s/consumer-" group) (.getBytes (:host kafka)))))

;; Выстраивание цепочки работы консьюмера:
;; -> обновления списка-консюмеров (initialized-zookeeper-path)
;; -> формирование полного списка консьюмеров (consumers-accumulator)
;; -> определение какие партишены нужно добавить, а какие удалить для текущего консюмера (partitions-solver)
;; -> супервизор партишен-консьюмеров, стартующий, стопающий их (partitions-supervisor)
;; Остановка консьюмера начинается с головы и продоходит по всей цепочке.
;; Завершением остановки следует считать закрытие channels-канала
(defn consumer
  [kafka {:keys [group topics init-offsets] :as params :or {init-offsets :latest}}]
  (let [me (introduce-myself kafka group)
        partitions (all-partitions kafka topics)
        initialized-zk-path (a/<!! (initialized-zookeeper-path @(:curator kafka) (format "/consumers/%s" group)))
        [stop-watch-consumers initial-consumers zk-consumers-ch] initialized-zk-path
        consumers-ch (consumers-accumulator initial-consumers zk-consumers-ch)
        partitions-changes-ch (partitions-solver consumers-ch me partitions)
        channels (partitions-supervisor kafka group init-offsets partitions-changes-ch)]
    [stop-watch-consumers channels]))
