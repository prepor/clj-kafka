(ns ru.prepor.clj-kafka.elastic
  (:require [clojure.core.async :as a]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [ru.prepor.clj-kafka :as core]
            [ru.prepor.utils :as utils])
  (:import [org.apache.curator.framework.recipes.cache PathChildrenCache
            PathChildrenCacheListener PathChildrenCache$StartMode PathChildrenCacheEvent$Type]
           [org.apache.zookeeper CreateMode]
           [org.apache.zookeeper KeeperException$NodeExistsException]))

(defn zookeeper-path
  [curator path & [{:keys [buf-or-n start-mode cache-data] :or {cache-data false}}]]
  (let [start-mode* (case start-mode
                      :normal PathChildrenCache$StartMode/NORMAL
                      :post-initialized-event
                      PathChildrenCache$StartMode/POST_INITIALIZED_EVENT)
        children-cache (PathChildrenCache. curator path cache-data)
        out (a/chan buf-or-n)]
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
    (utils/safe-go
     (let [init (loop [acc []]
                  (when-let [v (a/<! changes)]
                    (if (= :initialized (:type v))
                      (do
                        (log/debug "Initialized zookeeper path" path acc)
                        acc)
                      (recur (conj acc (:path v))))))]
       [stop-fn init changes]))))

(defn partition-consumer
  "Same as partition consumer from core, but it wait ack of last consumed message on
  stop. Returns stop-fn and core.async's channel"
  [kafka {:keys [buf-or-n] :as params}]
  (let [control-ch (a/chan)
        completed-wait-ch (a/chan)
        acks-ch (a/chan 10)
        ack-clb (fn [message]
                  (a/>!! acks-ch (:offset message)))
        out (a/chan buf-or-n)
        [init-offset messages] (core/partition-consumer kafka (assoc params
                                                                :control-ch control-ch
                                                                :ack-clb ack-clb
                                                                :buf-or-n 10))]
    (a/go-loop [consumed-offset nil acked-offset nil wait-last-ack? false]
      (if wait-last-ack?
        (do (log/debugf "Partition %s(%s) wait ack. Consumed offset: %s Current acked: %s"
                        (:topic params) (:partition params) consumed-offset acked-offset)
            (if (or (nil? consumed-offset)
                    (and acked-offset (<= consumed-offset acked-offset)))
              (do (a/close! acks-ch)
                  (a/close! completed-wait-ch))
              (recur consumed-offset (a/<! acks-ch) true)))
        (a/alt!
          acks-ch ([v] (recur consumed-offset v false))
          messages ([v] (if v
                          (do
                            (a/>! out v)
                            (recur (:offset v) acked-offset false))
                          (do
                            (a/close! out)
                            (recur consumed-offset acked-offset true))))
          :priority true)))
    [(fn [] (a/close! control-ch) completed-wait-ch)
     init-offset
     out]))

(defn my-partitions
  [me everybody all-partitions]
  (let [n (.indexOf (vec everybody) me)
        total (count everybody)]
    (for [[i v] (map-indexed vector all-partitions)
          :when (= n (mod i total))]
      v)))

(defn changed-partitions
  [old new]
  (let [old* (set old)
        new* (set new)
        added (seq (set/difference new* old*))
        removed (seq (set/difference old* new*))]
    (cond-> {}
            added (assoc :added added)
            removed (assoc :removed removed))))

(defn consumers-diff
  [me all-partitions consumers new-everybody]
  (let [old-partitions (keys consumers)
        new-partitions (my-partitions me new-everybody all-partitions)]
    (log/infof "Diff partitions for %s: old: %s new: %s" me old-partitions
               (pr-str new-partitions))
    (changed-partitions old-partitions new-partitions)))

(defn stop-diff
  [consumers]
  {:removed (keys consumers)})

(defn partition-path
  [consumer-params]
  (let [{:keys [group partition topic]} consumer-params]
    (format "/partitions/%s/%s/%s" group topic partition)))

(defn apply-added
  [kafka base-params added]
  (let [[topic partition] added
        consumer-params (assoc base-params :topic topic :partition partition)
        path (partition-path consumer-params)]
    (utils/safe-go
     (log/infof "Lock partition %s(%s) for myself" topic partition)
     (log/debug "Try to create zookeeper path" path)
     (try
       (->  @(:curator kafka)
            .create
            (.creatingParentsIfNeeded)
            (.withMode CreateMode/EPHEMERAL)
            (.forPath path))
       (log/infof "Locking partition successful %s(%s)" topic partition)
       (partition-consumer kafka consumer-params)
       (catch KeeperException$NodeExistsException e
         (log/warn "Zookeeper path already exists" path)
         (a/<! (a/timeout 100))
         nil)))))

(defn apply-removed
  [kafka base-params removed stop-fn]
  (let [[topic partition] removed
        consumer-params (assoc base-params :topic topic :partition partition)]
    (utils/safe-go
     (log/infof "Wait for partition %s(%s) complete consuming" topic partition)
     (utils/<? (stop-fn))
     (log/infof "Consuming of partition %s(%s) completed" topic partition)
     (-> @(:curator kafka) .delete (.forPath (partition-path consumer-params))))))

(defn apply-diff
  [kafka channels base-params consumers diff]
  (let [{:keys [added removed]} diff]
    (utils/safe-go
     (let [consumers-with-added
           (loop [consumers consumers [[topic partition :as added-one] & tail] (seq added)]
             (if added-one
               (if-let [[stop-fn init-offset  out]
                        (utils/<? (apply-added kafka base-params added-one))]
                 (do (a/>! channels {:topic topic
                                     :partition partition
                                     :init-offset init-offset
                                     :chan out})
                     (recur (assoc consumers added-one stop-fn) tail))
                 (recur consumers tail))
               consumers))]
       (loop [consumers consumers-with-added [removed-one & tail] (seq removed)]
         (if removed-one
           (do (utils/<? (apply-removed kafka base-params removed-one
                                        (consumers removed-one)))
               (recur (dissoc consumers removed-one) tail))
           consumers))))))

(defn introduce-myself
  [kafka group]
  (-> @(:curator kafka)
      .create
      (.creatingParentsIfNeeded)
      (.withMode CreateMode/EPHEMERAL_SEQUENTIAL)
      (.forPath (format "/consumers/%s/consumer-" group))))

(defn consumer
  [kafka {:keys [group topics init-offsets buf-or-n] :as params :or {init-offsets :latest}}]
  (let [partitions (-> (for [t (core/topics-metadata kafka topics)
                             partition (:partition-metadata t)]
                         [(:topic t) (:id partition)])
                       sort)
        channels (a/chan)
        me (introduce-myself kafka group)
        [consumers-stop-fn everybody consumer-changes]
        (utils/<??
         (initialized-zookeeper-path @(:curator kafka) (format "/consumers/%s" group)
                                     {:buf-or-n 10}))
        sorted-everybody (sort everybody)
        base-params {:group group
                     :init-offsets init-offsets
                     :buf-or-n buf-or-n}
        close-wait-ch (a/chan)
        update-everybody (fn [state event]
                           (if event
                             (let [op (case (:type event)
                                        :child-added #(sort (conj %1 %2))
                                        :child-removed #(sort (disj (set %1) %2)))]
                               (update-in state [:everybody] op (:path event)))
                             (assoc state :everybody nil)))]
    (a/go
      (loop [state {:everybody sorted-everybody :consumers {}}]
        (if (:everybody state)
          (if-let [diff (not-empty (consumers-diff me partitions
                                                   (:consumers state) (:everybody state)))]
            (let [[event _] (a/alts! [consumer-changes] :default :no)]
              (case event
                :no (let [new-consumers
                          (->>
                           diff
                           (apply-diff kafka channels base-params (:consumers state))
                           (a/<!))]
                      (if (utils/throwable? new-consumers)
                        (recur (assoc state :everybody nil))
                        (recur (assoc state :consumers new-consumers))))
                nil (recur (assoc state :everybody nil))
                (recur (update-everybody state event))))
            (recur (update-everybody state (a/<! consumer-changes))))
          (do (log/info "Stop consumer" params)
              (->> (stop-diff (:consumers state))
                   (apply-diff kafka channels base-params (:consumers state))
                   (utils/<?))
              (a/close! close-wait-ch)))))
    [(fn [] (a/close! channels) (consumers-stop-fn) (a/<!! close-wait-ch))
     channels]))
