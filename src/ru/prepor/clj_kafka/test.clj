(ns ru.prepor.clj-kafka.test
  (:import [java.util Properties]
           [org.I0Itec.zkclient ZkClient]
           [org.I0Itec.zkclient.serialize ZkSerializer]
           [org.apache.curator.test TestingServer]
           [kafka.common TopicAndPartition]
           [kafka.admin CreateTopicCommand]
           [kafka.server KafkaConfig KafkaServer]
           [org.apache.commons.io FileUtils])
  (:require [clojure.java.io :as io]))

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
                     "log.dir" (.getAbsolutePath (io/file (tmp-dir "kafka-log")))}]
    (KafkaServer. (KafkaConfig. (as-properties (assoc base-config "port" (str kafka-port))))
                  system-time)))

(defn wait-until-initialised
  [kafka-server topic]
  (let [topic-and-partition (TopicAndPartition. topic 0)]
    (while (not (.. kafka-server apis leaderCache keySet (contains topic-and-partition)))
      (Thread/sleep 500))))

(defn create-topic
  [zk-client topic & {:keys [partitions replicas]
                      :or   {partitions 1 replicas 1}}]
  (CreateTopicCommand/createTopic zk-client topic partitions replicas ""))

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
                             :kafka-port 9093
                             :topic "test"})
          zk (create-zookeeper config)
          kafka (create-broker config)
          topic (:topic config)]
      (try
        (.startup kafka)
        (with-open [zk-client (ZkClient. (str "127.0.0.1:" (:zookeeper-port config))
                                         500 500 string-serializer)]
          (create-topic zk-client topic)
          (wait-until-initialised kafka topic)
          (f))
        (finally (do (.shutdown kafka)
                     (.awaitShutdown kafka)
                     (.stop zk)
                     (FileUtils/deleteDirectory (io/file (tmp-dir)))))))))
