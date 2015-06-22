(ns ru.prepor.clj-kafka.tracers.prom
  "Prometheus metrics"
  (:require [ru.prepor.clj-kafka.tracer :as trace]
            [com.stuartsierra.component :as component])
  (:import [io.prometheus.client Counter]))

(defrecord MetricsTracer [counter]
  trace/Tracer
  (started [this group topic partition init-offset])
  (state-changed [this group topic partition state])
  (ack-received [this group topic partition offset])
  (message-received [this group topic partition offset]
    (-> counter
        (.labels (into-array [group topic (str partition)]))
        (.inc)))
  (wait-acks-progress [this group topic partition consumed-offset acked-offset])
  (consumer-started [this  group topics])
  (consumer-failed [this group topics exception])
  (consumer-stopped [this group topics])
  component/Lifecycle
  (start [this] (assoc this :counter
                       (-> (Counter/build)
                           (.name "kafka_consumed_total")
                           (.help "kafka consumer metric")
                           (.labelNames (into-array ["group" "topic" "partition"]))
                           (.register))))
  (stop [this] this))

(defn mk[] (map->MetricsTracer {}))
