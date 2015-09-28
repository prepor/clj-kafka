(ns ru.prepor.clj-kafka.tracers.prom
  "Prometheus metrics"
  (:require [ru.prepor.clj-kafka.tracer :as trace]
            [defcomponent :refer [defcomponent]]
            [com.stuartsierra.component :as component])
  (:import [io.prometheus.client Counter]))

(defcomponent metrics-tracer []
  []
  (start [this] (assoc this :counter
                       (-> (Counter/build)
                           (.name "kafka_consumed_total")
                           (.help "kafka consumer metric")
                           (.labelNames (into-array ["group" "topic" "partition"]))
                           (.register))))
  (stop [this] this)
  trace/Tracer
  (started [this group topic partition init-offset])
  (state-changed [this group topic partition state])
  (ack-received [this group topic partition offset])
  (message-received
   [this group topic partition offset]
   (-> (:counter this)
       (.labels (into-array [group topic (str partition)]))
       (.inc)))
  (wait-acks-progress [this group topic partition consumed-offset acked-offset])
  (consumer-started [this  group topics])
  (consumer-failed [this group topics exception])
  (consumer-stopped [this group topics]))
