(ns ru.prepor.clj-kafka.elastic-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer :all]
            [ru.prepor.clj-kafka.tracers.state :as state-tracer]
            [ru.prepor.clj-kafka :as kafka]
            [ru.prepor.clj-kafka.elastic :as elastic]
            [ru.prepor.clj-kafka.test-utils :refer [*kafka* *kafka-producer*
                                                    with-env async-res]]))

(use-fixtures :each with-env)

(def consumer-params {:group "test"
                      :topics ["test1" "test2" "test3"]
                      :init-offsets :latest})

(deftest basic
  (kafka/send *kafka-producer* [{:topic "test1" :key "1" :value "init message"}])
  (kafka/send *kafka-producer* [{:topic "test2" :key "1" :value "init message"}])
  (kafka/send *kafka-producer* [{:topic "test3" :key "1" :value "init message"}]) ()
  (let [[consumer1-stop consumer1-channels] (elastic/consumer *kafka* consumer-params)
        consumer1-topics (->> (a/take 3 consumer1-channels)
                              (a/into [])
                              (async-res))
        consumer1-test1-messages (:chan
                                  (some #(when (= "test1" (:topic %)) %) consumer1-topics))
        consumer1-test2-messages (:chan
                                  (some #(when (= "test2" (:topic %)) %) consumer1-topics))]
    (is (= #{"test1" "test2" "test3"} (set (map :topic consumer1-topics))))

    (kafka/send *kafka-producer* [{:topic "test1" :key "1" :value "hello!"}])
    (kafka/send *kafka-producer* [{:topic "test2" :key "1" :value "hi!"}])
    (let [res (async-res consumer1-test1-messages)]
      (is (= "hello!" (String. (:value res))))
      (a/<!! (kafka/commit res)))

    (let [res-test2-from-consumer1 (async-res consumer1-test2-messages)
          [consumer2-stop consumer2-channels] (elastic/consumer *kafka* consumer-params)]
      (is (= "hi!" (String. (:value res-test2-from-consumer1))))
      (kafka/send *kafka-producer* [{:topic "test2" :key "1" :value "hi2!"}])
      (Thread/sleep 200)
      (a/<!! (kafka/commit res-test2-from-consumer1))
      (let [consumer2-test2 (async-res consumer2-channels)]
        (is (= "test2" (:topic consumer2-test2)))
        (is (nil? (async-res consumer1-test2-messages)))
        (let [res (async-res (:chan consumer2-test2))]
          (is (= "hi2!" (String. (:value res))))
          (a/<!! (kafka/commit res))))
      (consumer1-stop)
      (consumer2-stop))))
