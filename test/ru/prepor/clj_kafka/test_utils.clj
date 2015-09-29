(ns ru.prepor.clj-kafka.test-utils
  (:require [clojure.core.async :as a]
            [clojure.test :refer :all]
            [com.stuartsierra.component :as component]
            [ru.prepor.clj-kafka :as kafka]
            [ru.prepor.clj-kafka.tracer :as tracer]
            [ru.prepor.clj-kafka.test :as test-kafka]
            [ru.prepor.utils :as utils]))

(def broker-config {:zookeeper-port 2182
                    :kafka-port 9093})

(def config {:brokers-list ["localhost:9093"]
             :zookeeper {:namespace "test"
                         :connect-string "localhost:2182"}})
(def producer-config {:brokers-list ["localhost:9093"]})

(defn async-res
  ([ch] (async-res ch 5))
  ([ch seconds]
   (a/alt!!
     (a/timeout (* seconds 5000)) (throw (Exception. "Timeout"))
     ch ([v ch] (utils/safe-res v)))))

(def ^:dynamic *kafka*)
(def ^:dynamic *kafka-producer*)

(defn test-consumer []
  (-> (kafka/kafka)
      (assoc :defcomponent/specs [:dependant tracer/nil-tracer :tracer] [:dependant kafka/in-memory :storage])))

(defn test-producer [] (kafka/kafka-producer producer-config))

(defn with-components
  [f]
  (let [system (defcomponent/system [test-consumer test-producer] {:start true})]
    (binding [*kafka* (get system test-consumer)
              *kafka-producer* (get system test-producer)]
      (try
        (f)
        (finally
          (component/stop system))))))


(defn with-test-broker
  [f]
  ((test-kafka/with-test-broker broker-config) f))

(defn with-env
  [f]
  ((join-fixtures [with-test-broker with-components]) f))
