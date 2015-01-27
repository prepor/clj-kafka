(ns ru.prepor.clj-kafka
  (:require [clj-kafka.producer :as kafka-producer]
            [clojure.core.async :as a]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [ru.prepor.utils :as utils]
            [taoensso.carmine :as car :refer (wcar)])
  (:import [java.nio ByteBuffer]
           [java.util Properties HashMap]
           [kafka.api FetchRequestBuilder PartitionOffsetRequestInfo]
           [kafka.cluster Broker]
           [kafka.common TopicAndPartition]
           [kafka.javaapi OffsetResponse PartitionMetadata TopicMetadata
            TopicMetadataResponse TopicMetadataRequest OffsetRequest]
           [kafka.javaapi.consumer SimpleConsumer]
           [kafka.message MessageAndMetadata MessageAndOffset]
           [kafka.producer KeyedMessage]
           [org.apache.commons.pool2 PooledObjectFactory]
           [org.apache.commons.pool2.impl GenericObjectPool DefaultPooledObject]
           [org.apache.curator.framework CuratorFrameworkFactory]
           [org.apache.curator.retry ExponentialBackoffRetry])
  (:refer-clojure :exclude [send]))

(defrecord Message [kafka ack-clb group topic partition offset key value])

(defprotocol OffsetsStorage
  (offset-read [this group topic partition-id])
  (offset-write [this group topic partition-id offset]))

(defn ack
  [message]
  (when-let [clb (:ack-clb message)]
    (clb message)))

(defn commit
  [message]
  (offset-write (get-in message [:kafka :storage])
                (:group message) (:topic message) (:partition message)
                (inc (:offset message)))
  (ack message))

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
          (log/warnf "Error while request to broker %s: %s" broker (.getMessage e))))
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
        topic-meta (some #(when (= topic (:topic %)) %) m)
        partition (some #(when (= partition-id (:id %)) %) (:partition-metadata topic-meta))]
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

(defn init-offset
  [kafka group topic partition offset-position]
  (or (offset-read (:storage kafka) group topic (:id partition))
      (init-offset* kafka topic partition offset-position)))

(defn partition-messages
  [kafka topic partition offset & [{:keys [ack-clb group]}]]
  (let [req (-> (FetchRequestBuilder.)
                (.clientId (format "flock-clj-%s-%s" topic (:id partition)))
                (.addFetch topic (:id partition) offset 1000000)
                (.build))
        to-message (fn [m]
                     (let [offset (.offset m)
                           msg (.message m)
                           payload-byte-buffer (.payload msg)
                           payload-byte-array (byte-array (.remaining payload-byte-buffer))
                           key-byte-buffer (.key msg)
                           key-byte-array (when key-byte-buffer
                                            (byte-array (.remaining key-byte-buffer)))]
                       (.get payload-byte-buffer payload-byte-array)
                       (when key-byte-buffer
                         (.get key-byte-buffer key-byte-array))
                       (map->Message {:kafka kafka
                                      :group group
                                      :ack-clb ack-clb
                                      :topic topic
                                      :offset offset
                                      :partition (:id partition)
                                      :key key-byte-array
                                      :value payload-byte-array})))
        res (request kafka (:leader partition) #(.fetch % req))]
    (if (or (nil? res) (.hasError res))
      ;; in case of error just returns empty messages coll and reinit broker's info
      (do
        (log/warnf "Error (%s) while fetching messages for topic %s partition %s offset %s from %s"
                   (when res (.errorCode res topic (:id partition)))
                   topic (:id partition) offset (:leader partition))
        [[] (refresh-partition kafka topic (:id partition)) offset])
      (let [messages (-> res (.messageSet topic (:id partition))
                         (.iterator) iterator-seq
                         (->> (mapv to-message)))]
        (log/debugf "Received %s messages from %s(%s) with offset %s" (count messages)
                    topic partition offset)
        [messages partition (if (seq messages) (inc (:offset (last messages))) offset)]))))

(defn get-messages
  [kafka topic]
  (apply concat
         (for [p (-> (topics-metadata kafka [topic]) first :partition-metadata)
               :when p
               :let [init-offset (init-offset* kafka topic p :earliest)]]
           (loop [res [] partition p offset init-offset]
             (let [[messages partition offset]
                   (partition-messages kafka topic partition offset)]
               (if (seq messages)
                 (recur (concat res messages) partition offset)
                 res))))))

(defn partition-consumer
  "Returns the core.async's channel. Stops consuming after control-ch closed"
  [kafka {:keys [topic partition control-ch ack-clb group init-offsets buf-or-n]}]
  (let [ch (a/chan buf-or-n)
        partition-meta (refresh-partition kafka topic partition)
        init-offset (init-offset kafka group topic partition-meta init-offsets)]
    (log/debug "Initialized parttion consumer" partition-meta init-offset)
    (a/go
      (try
        (loop [partition-meta partition-meta offset init-offset]
          (let [[messages partition-meta offset]
                (partition-messages kafka topic partition-meta offset {:ack-clb ack-clb
                                                                       :group group})
                tick-result
                (if (seq messages)
                  (loop [[m & messages] messages]
                    (if m
                      (a/alt!
                        [[ch m]] ([] (recur messages))
                        control-ch :stopped)
                      :next))
                  (a/alt!
                    (a/timeout 1000) :next
                    control-ch :stopped))]
            (case tick-result
              :next (recur partition-meta offset)
              :stopped (a/close! ch))))
        (catch Exception e
          (log/error e "Error while consuming" group topic partition-meta)
          (a/close! ch))))
    [init-offset ch]))

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
              offset (or (offset-read (:storage kafka) group topic (:id p)) 0)]
        :let [lag (- log offset)]]
    {:topic topic :partition (:id p) :offset offset :log log :lag lag}))

(defn redis-offset-key
  [group topic partition-id]
  (format "kafka-offsets:%s-%s-%s" group topic partition-id))

(defn new-redis
  [config]
  (reify OffsetsStorage
    (offset-read [_ group topic partition-id]
      (when-let [res (wcar config (car/get (redis-offset-key group topic partition-id)))]
        (Long/valueOf res)))
    (offset-write [_ group topic partition-id offset]
      (wcar config (car/set (redis-offset-key group topic partition-id) offset)))))

(defrecord Kafka [config pool storage curator]
  component/Lifecycle
  (start [this]
    (assoc this
      :pool (atom {})
      :curator (delay (-> (CuratorFrameworkFactory/builder)
                          (.namespace (get-in config [:zookeeper :namespace]))
                          (.connectString (get-in config [:zookeeper :connect-string]))
                          (.retryPolicy (ExponentialBackoffRetry. 100 10))
                          (.build)
                          (doto (.start) (.blockUntilConnected))))
      :storage (cond
                (get-in config [:storage :redis])
                (new-redis (get-in config [:storage :redis])))))
  (stop [this]
    (doseq [[_ v] @pool]
      (.close v))
    (when (realized? curator)
      (.close @curator))
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
          "serializer.class" (get config :serializer "kafka.serializer.StringEncoder")
          "partitioner.class" (get config :partitioner "kafka.producer.DefaultPartitioner")
          "producer.type" "sync"
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
                          (KeyedMessage. topic key value)))
        (finally
          (.returnObject pool producer))))))

(defn new-kafka-producer
  [config]
  (map->KafkaProducer {:config config}))
