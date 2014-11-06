(ns ru.prepor.clj-kafka
  (:require [clojure.string :as str]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as log]
            [ru.prepor.utils :as utils]
            [clojure.core.async :as a]
            [taoensso.carmine :as car :refer (wcar)]
            [clj-kafka.producer :as kafka-producer])
  (:import [java.nio ByteBuffer]
           [java.util Properties HashMap]
           [kafka.common TopicAndPartition]
           [kafka.api FetchRequestBuilder PartitionOffsetRequestInfo]
           [kafka.message MessageAndMetadata MessageAndOffset]
           [kafka.javaapi OffsetResponse PartitionMetadata TopicMetadata
            TopicMetadataResponse TopicMetadataRequest OffsetRequest]
           [kafka.cluster Broker]
           [kafka.producer KeyedMessage]
           [kafka.javaapi.consumer SimpleConsumer]
           [org.apache.commons.pool2 PooledObjectFactory]
           [org.apache.commons.pool2.impl GenericObjectPool DefaultPooledObject])
  (:refer-clojure :exclude [send]))

(defrecord Message [topic offset partition key value kafka])

(defn as-properties
  [m]
  (let [props (Properties. )]
    (doseq [[n v] m] (.setProperty props n v))
    props))

(defprotocol ToClojure
  (to-clojure [x] "Converts type to Clojure structure"))

(extend-protocol ToClojure
  Broker
  (to-clojure [x]
    {:connect (.getConnectionString x)
     :host (.host x)
     :port (.port x)
     :id (.id x)})

  PartitionMetadata
  (to-clojure [x]
    {:id (.partitionId x)
     :leader (to-clojure (.leader x))
     :replicas (map to-clojure (.replicas x))
     :in-sync-replicas (map to-clojure (.isr x))
     :error-code (.errorCode x)})

  TopicMetadata
  (to-clojure [x]
    {:topic (.topic x)
     :partition-metadata (map to-clojure (.partitionsMetadata x))})

  TopicMetadataResponse
  (to-clojure [x]
    (map to-clojure (.topicsMetadata x))))

(defn pool-key
  [broker]
  {:host (:host broker) :port (:port broker)})

(defn request
  [kafka broker f & args]
  (let [pool (:pool kafka)
        k (pool-key broker)]
    (if-let [consumer (get @pool k)]
      (try
        (apply f consumer args)
        (catch Exception e
          (log/warn (format "Error while request to broker %s: %s" broker (.getMessage e)))))
      (do (locking pool
            (swap! pool assoc k (SimpleConsumer. (:host broker) (:port broker)
                                                 100000 (* 64 1024)
                                                 "flock-clj-consumer")))
          (apply request kafka broker f args)))))

(defn any-request
  [kafka f & args]
  (let [brokers (for [b (-> kafka :config :brokers-list)
                      :let [[h p] (str/split b #":")]]
                  {:host h :port (Integer/valueOf p)})]
    (if-let [res (some #(apply request kafka % f args) brokers)]
      res
      (throw (Exception. (format "Can't make request to any of brokers %s"
                                 (pr-str brokers)))))))

(defn topics-metadata
  [kafka topics]
  (let [req (TopicMetadataRequest. topics)]
    (to-clojure (any-request kafka #(.send % req)))))

(defn refresh-partition
  [kafka topic partition-id]
  (let [m (topics-metadata kafka [topic])
        topic (some #(when (= topic (:topic %)) %) m)
        partition (some #(when (= partition-id (:id %)) %) (:partition-metadata topic))]
    (when-not partition
      (throw (Exception. (format "Can't find new broker for topic %s parition %s in %s"
                                 topic partition-id m))))
    partition))

(defn init-offset*
  [kafka topic partition offset-position]
  (let [op {:latest -1 :earliest -2}
        tp (TopicAndPartition. topic (:id partition))
        pori (PartitionOffsetRequestInfo. (offset-position op) 1)
        hm (HashMap. {tp pori})
        req (OffsetRequest. hm (kafka.api.OffsetRequest/CurrentVersion) "flock-clj-id")]
    (let [response (request kafka (:leader partition) #(.getOffsetsBefore % req))]
      (first (.offsets response topic (:id partition))))))

(defn offset-key
  [group topic partition-id]
  (format "kafka-offsets:%s-%s-%s" group topic partition-id))

(defn read-offset
  [kafka group topic partition-id]
  (when-let [res (wcar (:redis kafka) (car/get (offset-key group topic partition-id)))]
    (Long/valueOf res)))

(defn commit-offset
  [kafka group topic partition-id offset]
  (wcar (:redis kafka) (car/set (offset-key group topic partition-id) offset)))

(defn commit-message
  [group message]
  (commit-offset (:kafka message) group (:topic message) (:partition message)
                 (inc (:offset message))))

(defn init-offset
  [kafka group topic partition offset-position]
  (or (read-offset kafka group topic (:id partition))
      (init-offset* kafka topic partition offset-position)))

(defn partition-messages
  [kafka topic partition offset]
  (let [req (-> (FetchRequestBuilder.)
                (.clientId (format "flock-clj-%s-%s" topic (:id partition)))
                (.addFetch topic (:id partition) offset 1000000)
                (.build))
        to-message (fn [m]
                     (let [offset (.offset m)
                           msg (.message m)
                           bb (.payload msg)
                           b (byte-array (.remaining bb))]
                       (.get bb b)
                       (Message. topic offset (:id partition) (.key msg) b kafka)))
        res (request kafka (:leader partition) #(.fetch % req))]
    (if (or (nil? res) (.hasError res))
      ;; in case of error just returns empty messages coll and reinit broker's info
      (do
        (log/warn (format "Error (%s) while fetching messages for topic %s partition %s offset %s from %s"
                          (when res (.errorCode res topic (:id partition)))
                          topic (:id partition) offset (:leader partition)))
        [[] (refresh-partition kafka topic (:id partition)) offset])
      (let [messages (-> res (.messageSet topic (:id partition))
                         (.iterator) iterator-seq
                         (->> (mapv to-message)))]
        [messages partition (if (seq messages) (inc (:offset (last messages))) offset)]))))

(defn get-messages
  [kafka topic]
  (apply concat
         (for [p (-> (topics-metadata kafka [topic]) first :partition-metadata)
               :let [init-offset (init-offset* kafka topic p :earliest)]]
           (loop [res [] partition p offset init-offset]
             (let [[messages partition offset]
                   (partition-messages kafka topic partition offset)]
               (if (seq messages)
                 (recur (concat res messages) partition offset)
                 res))))))

(defn constant-supervisor
  "The most primitive implementation of distributive client. Based on total-n and
  current-n configuration, without any automatic rebalancing.
  Requests are executed in go-blocks thread-pool, with blocking IO, but without _wait-for_
  blocking."
  [kafka {:keys [group topics total-n current-n init-offsets] :or {init-offsets :latest}}]
  (let [m (topics-metadata kafka topics)
        running? (atom true)
        channels (a/chan)
        threads (for [t m
                      p (:partition-metadata t)
                      :when (= current-n (mod (:id p) total-n))
                      :let [ch (a/chan 100)
                            topic (:topic t)
                            init-offset (init-offset kafka group topic p init-offsets)]]
                  (utils/safe-go
                   (a/>! channels {:topic topic :partition (:id p) :init-offset init-offset
                                   :chan ch})
                   (loop [partition p offset init-offset]
                     (when @running?
                       (let [[messages partition offset]
                             (partition-messages kafka topic partition offset)]
                         (if (seq messages)
                           (doseq [m messages]
                             (a/>! ch m))
                           (a/<! (a/timeout 1000)))
                         (recur partition offset))))
                   (a/close! ch)))
        stop! (fn [] (reset! running? false) (a/close! channels))]
    (doall threads)
    ;; Closes all channels on first failure
    (let [threads-ch (a/merge threads)]
      (a/go-loop []
        (when-let [v (a/<! threads-ch)]
          (when (utils/throwable? v)
            (log/error v (format "Stop kafka consumer %s" topics))
            (stop!))
          (recur))))
    [stop! channels]))

(defn all-messages
  "Receives all messages in topics at the moment of call. After that closes channels"
  [kafka {:keys [group topics]}]
  (let [m (topics-metadata kafka topics)
        channels (a/chan)]
    (doseq [t m
            p (:partition-metadata t)
            :let [ch (a/chan 100)
                  topic (:topic t)
                  init-offset (init-offset kafka group topic p :earliest)
                  last-offset (init-offset* kafka topic p :latest)]]
      (a/put! channels {:topic topic :partition (:id p)
                        :init-offset init-offset :last-offset last-offset
                        :chan ch})
      (utils/safe-go
       (loop [partition p offset init-offset]
         (when (< offset last-offset)
           (let [[messages partition offset]
                 (partition-messages kafka topic partition offset)]
             (when (seq messages)
               (doseq [m messages]
                 (a/>! ch m)))
             (recur partition offset))))
       (a/close! ch)))
    (a/close! channels)
    channels))

(defn channels->channel
  "Combines channel of channels to one channel"
  [channels]
  (let [out (a/chan 100)]
    (utils/safe-go
     (loop [channels-for-read [channels]]
       (if (seq channels-for-read)
         (let [[v port] (a/alts! channels-for-read)]
           (cond
            (nil? v) (recur (-> (set channels-for-read) (disj port) vec))
            (= channels port) (recur (conj channels-for-read (:chan v)))
            :else (do (a/>! out v) (recur channels-for-read))))
         (a/close! out))))
    out))

(defn status
  [kafka group topics]
  (for [t (topics-metadata kafka topics)
        p (:partition-metadata t)
        :let [topic (:topic t)
              log (init-offset* kafka topic p :latest)
              offset (or (read-offset kafka group topic (:id p)) 0)]
        :let [lag (- log offset)]]
    {:topic topic :partition (:id p) :offset offset :log log :lag lag}))

(defrecord Kafka [config pool redis]
  component/Lifecycle
  (start [this]
    (assoc this
      :pool (atom {})
      :redis (-> config :redis)))
  (stop [this]
    (doseq [[_ v] @pool]
      (.close v))
    this))

(defn new-kafka
  [config]
  (map->Kafka {:config config}))

(defn kafka-producer-factory
  [config]
  (reify PooledObjectFactory
    (makeObject [_]
      (DefaultPooledObject.
        (kafka-producer/producer
         {"metadata.broker.list" (clojure.string/join "," (:brokers-list config))
          "serializer.class" "kafka.serializer.DefaultEncoder"
          "partitioner.class" "kafka.producer.DefaultPartitioner"
          "request.required.acks" "1"})))
    (destroyObject [_ pooled-obj]
      (.close (.getObject pooled-obj)))
    (validateObject [_ _pooled-obj] true)
    (activateObject [_ _])
    (passivateObject [_ _])))

(defprotocol KafkaSend
  (send [_ messages]))

(defrecord KafkaProducer [config pool]
  component/Lifecycle
  (start [this]
    (assoc this :pool
           (GenericObjectPool. (kafka-producer-factory config))))
  (stop [this]
    (.close pool)
    this)
  KafkaSend
  (send [_ messages]
    (let [producer (.borrowObject pool)]
      (try
        (.send producer (for [{:keys [topic key value]} messages]
                          (KeyedMessage. topic
                                         (if (string? key) (.getBytes key) key)
                                         (if (string? value) (.getBytes value) value))))
        (finally
          (.returnObject pool producer))))))

(defn new-kafka-producer
  [config]
  (map->KafkaProducer {:config config}))
