(ns ru.prepor.clj-kafka.test-utils
  (:require [clojure.test :as test]
            [ru.prepor.utils :as utils]
            [clojure.test :refer :all]
            [ru.prepor.clj-kafka :as kafka]
            [ru.prepor.clj-kafka.tracers.pub :refer [pub-tracer]]
            [ru.prepor.clj-kafka.tracers.state :refer [state-tracer]]
            [ru.prepor.clj-kafka.tracers.log :refer [log-tracer]]
            [com.stuartsierra.component :as component]
            [taoensso.carmine :as car :refer [wcar]]
            [ru.prepor.clj-kafka.test :as test-kafka]
            [clojure.core.async :as a]))

(def broker-config {:zookeeper-port 2182
                    :kafka-port 9093})

(def config {:storage {:redis {:pool {} :spec {:host "127.0.0.1" :port 6379 :db 5}}}
             :brokers-list ["localhost:9093"]
             :zookeeper {:namespace "test"
                         :connect-string "localhost:2182"}})
(def producer-config {:brokers-list ["localhost:9093"]})

(defn async-res
  ([ch] (async-res ch 5))
  ([ch seconds]
     (a/alt!!
       (a/timeout (* seconds 5000)) (throw (Exception. "Timeout"))
       ch ([v ch] (utils/safe-res v)))))

(defn with-truncated-redis
  [f]
  (wcar (:redis config) (car/flushall))
  (f))

(def ^:dynamic *kafka*)
(def ^:dynamic *kafka-producer*)

(defn tracer []
  (-> (pub-tracer) (assoc :state (component/start (state-tracer))
                          :log (component/start (log-tracer)))
      (component/start)))

(defn with-components
  [f]
  (binding [*kafka* (component/start (assoc (kafka/new-kafka config) :tracer (tracer)))
            *kafka-producer* (component/start (kafka/new-kafka-producer producer-config))]
    (try
      (f)
      (finally
        (component/stop *kafka*)
        (component/stop *kafka-producer*)))))


(defn with-test-broker
  [f]
  ((test-kafka/with-test-broker broker-config) f))

(defn with-env
  [f]
  ((join-fixtures [with-truncated-redis with-test-broker with-components]) f))
