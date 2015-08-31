(ns ru.prepor.clj-kafka-test
  (:require [ru.prepor.clj-kafka :as kafka]
            [ru.prepor.utils :as utils]
            [clojure.core.async :as a]
            [ru.prepor.clj-kafka.test-utils :refer [with-env *kafka-producer* *kafka*
                                                    async-res]]
            [clojure.test :refer :all]))

(use-fixtures :each with-env)

(def consumer-params {:group "test"
                      :topic "test"
                      :partition 0
                      :init-offsets :latest})

(deftest basic
  (kafka/send *kafka-producer* [{:topic "test" :key "1" :value "init message"}])

  (let [control-ch (a/chan)
        [init-offset messages] (a/<!! (kafka/partition-consumer *kafka* (assoc consumer-params
                                                                               :control-ch control-ch)))]
    (is (= 1 init-offset))
    (kafka/send *kafka-producer* [{:topic "test" :key "1" :value "hello!"}])

    (let [res (async-res messages)]
      (is (= "hello!" (String. (:value res))))
      (a/<!! (kafka/commit res)))

    (a/close! control-ch)
    (is (nil? (async-res messages))))

  (kafka/send *kafka-producer* [{:topic "test" :key "1" :value "привет!"}])

  (let [control-ch (a/chan)
        [_ messages] (a/<!! (kafka/partition-consumer *kafka* (assoc consumer-params
                                                                     :control-ch control-ch)))
        res (async-res messages)]
    (is (= "1" (String. (:key res))))
    (is (= "привет!" (String. (:value res))))
    (a/close! control-ch))
  )

(deftest all-messages
  (kafka/send *kafka-producer* [{:topic "test" :key "1" :value "hello!"}])
  (let [channels (a/<!! (kafka/all-messages *kafka*
                                            {:group "test"
                                             :topics ["test"]}))
        messages (async-res (a/into [] (kafka/channels->channel channels)))]
    (is (= ["hello!"] (map #(String. (:value %)) messages))))
  )
