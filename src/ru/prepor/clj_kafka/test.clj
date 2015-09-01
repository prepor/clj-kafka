(ns ru.prepor.clj-kafka.test
  (:import [java.util Properties]
           [org.I0Itec.zkclient ZkClient]
           [org.I0Itec.zkclient.serialize ZkSerializer]
           [org.apache.curator.test TestingServer]
           [kafka.common TopicAndPartition]
           [kafka.admin AdminUtils]
           [kafka.server KafkaConfig KafkaServer]
           [org.apache.commons.io FileUtils])
  (:require [clojure.java.io :as io]
            [ru.prepor.clj-kafka :as kafka]
            [clojure.core.async :as a]
            [com.stuartsierra.component :as component]))

(def system-time (proxy [kafka.utils.Time] []
                   (milliseconds [] (System/currentTimeMillis))
                   (nanoseconds [] (System/nanoTime))
                   (sleep [ms] (Thread/sleep ms))))

(defn as-properties
  [m]
  (let [props (Properties. )]
    (doseq [[n v] m] (.setProperty props n v))
    props))

(defn tmp-dir
  [& parts]
  (.getPath (apply io/file (System/getProperty "java.io.tmpdir") "clj-kafka" parts)))

;; enable.zookeeper doesn't seem to be used- check it actually has an effect
(defn create-broker
  [{:keys [kafka-port zookeeper-port]}]
  (let [base-config {"broker.id" "0"
                     "port" "9999"
                     "host.name" "localhost"
                     "zookeeper.connect" (str "127.0.0.1:" zookeeper-port)
                     "enable.zookeeper" "true"
                     "log.flush.interval.messages" "1"
                     "auto.create.topics.enable" "true"
                     "default.replication.factor" "1"
                     "log.dir" (.getAbsolutePath (io/file (tmp-dir "kafka-log")))}]
    (KafkaServer. (KafkaConfig. (as-properties (assoc base-config "port" (str kafka-port))))
                  system-time)))

(def string-serializer (proxy [ZkSerializer] []
                         (serialize [data] (.getBytes data "UTF-8"))
                         (deserialize [bytes] (when bytes
                                                (String. bytes "UTF-8")))))

(defn create-zookeeper
  [{:keys [zookeeper-port]}]
  (TestingServer. zookeeper-port true))

(defn with-test-broker
  "Creates an in-process broker that can be used to test against"
  [& [config]]
  (fn [f]
    (FileUtils/deleteDirectory (io/file (tmp-dir)))
    (let [config (or config {:zookeeper-port 2182
                             :kafka-port 9093})
          zk (create-zookeeper config)
          kafka (create-broker config)]
      (try
        (.startup kafka)
        (with-open [zk-client (ZkClient. (str "127.0.0.1:" (:zookeeper-port config))
                                         500 500 string-serializer)]
          (f))
        (finally (do (.shutdown kafka)
                     (.awaitShutdown kafka)
                     (.stop zk)
                     (FileUtils/deleteDirectory (io/file (tmp-dir)))))))))

(defrecord StubbedStorage [commits offsets]
  component/Lifecycle
  (start [this]
    (assoc this
           :offsets (atom (:init-offsets this {}))
           :commits (a/chan)))
  (stop [this] this)
  kafka/OffsetsStorage
  (offset-read [this group topic partition-id]
    (a/go (get @offsets [group topic partition-id])))
  (offset-write [this group topic partition-id offset]
    (a/go
      (swap! offsets assoc [group topic partition-id] offset)
      (a/>! commits {:group group :topic topic :partition-id partition-id :offset offset}))))

(defn stubbed-storage
  [init-offsets]
  (map->StubbedStorage {:init-offsets init-offsets}))
