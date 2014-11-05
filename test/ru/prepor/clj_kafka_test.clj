(ns ru.prepor.clj-kafka-test
  (:require [ru.prepor.clj-kafka :as kafka]
            [ru.prepor.clj-kafka.test :as test-kafka]
            [ru.prepor.utils :as utils]
            [clojure.core.async :as a]
            [taoensso.carmine :as car :refer [wcar]]
            [com.stuartsierra.component :as component]
            [clojure.test :refer :all]))

(def broker-config {:zookeeper-port 2182
                    :kafka-port 9093
                    :topic "test"})

(def config {:redis {:pool {} :spec {:host "127.0.0.1" :port 6379 :db 5}}
             :brokers-list ["localhost:9093"]})
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

(defn with-components
  [f]
  (binding [*kafka* (component/start (kafka/new-kafka config))
            *kafka-producer* (component/start (kafka/new-kafka-producer producer-config))]
    (try
      (f)
      (finally
        (component/stop *kafka*)
        (component/stop *kafka-producer*)))))

(use-fixtures :each with-truncated-redis
  (test-kafka/with-test-broker broker-config) with-components)

(deftest basic
  (let [[close-f channels] (kafka/constant-supervisor *kafka*
                                                      {:group "test"
                                                       :topics ["test"]
                                                       :total-n 1
                                                       :current-n 0})
        messages (async-res channels)]
    (kafka/send *kafka-producer* [{:topic "test" :key "1" :value "hello!"}])

    (let [res (async-res messages)]
      (is (= "hello!" (String. (:value res))))
      (kafka/commit-message "test" res))

    (close-f)
    (is (nil? (async-res messages)))
    (is (nil? (async-res channels))))

  (kafka/send *kafka-producer* [{:topic "test" :key "1" :value "hello2!"}])

  (let [[close-f channels] (kafka/constant-supervisor *kafka* {:group "test"
                                                               :topics ["test"]
                                                               :total-n 1
                                                               :current-n 0})
        messages (async-res channels)]
    (let [res (async-res messages)]
      ;; TODO key is broken?..
      ;; (is (= "1" (String. (:key res))))
      (is (= "hello2!" (String. (:value res)))))
    (close-f)))

(deftest all-messages
  (kafka/send *kafka-producer* [{:topic "test" :key "1" :value "hello!"}])
  (let [channels (kafka/all-messages *kafka*
                                     {:group "test"
                                      :topics ["test"]})
        messages (async-res (a/into [] (kafka/channels->channel channels)))]
    (is (= ["hello!"] (map #(String. (:value %)) messages)))))
