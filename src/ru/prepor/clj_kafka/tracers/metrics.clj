(ns ru.prepor.clj-kafka.tracers.metrics
  (:require [com.stuartsierra.component :as component]
            [metrics.core :as metrics]
            [metrics.meters :as meters]
            [ru.prepor.clj-kafka.tracer :as trace]))

;; [1 2 3] => [[1] [1 2] [1 2 3]]
(defn seq-reduction
  ([coll] (seq-reduction [] (seq coll)))
  ([acc tail]
     (when (seq tail)
       (let [acc* (conj acc (first tail))]
         (cons acc* (lazy-seq (seq-reduction acc* (rest tail))))))))

(defn partition-meter
  [registry group topic partition type]
  (let [partition* (str partition)]
    (doseq [t (seq-reduction [group topic partition*])]
      (meters/mark! (meters/meter registry (conj t type))))))

(defrecord MetricsTracer [registry]
  trace/Tracer
  (started [this group topic partition init-offset])
  (state-changed [this group topic partition state]
    (let [meter (partial partition-meter registry group topic partition)]
      (case state
        :try-to-lock (meter "lock-tries")
        :cant-lock (meter "lock-fails")
        :started (meter "consuming-starts")
        :completed (meter "consuming-stops")
        nil)))
  (ack-received [this group topic partition offset]
    (partition-meter registry group topic partition "acsk"))
  (message-received [this group topic partition offset]
    (partition-meter registry group topic partition "messages"))
  (wait-acks-progress [this group topic partition consumed-offset acked-offset])
  (consumer-started [this  group topics])
  (consumer-failed [this group topics exception])
  (consumer-stopped [this group topics])
  component/Lifecycle
  (start [this] (assoc this :registry (metrics/new-registry)))
  (stop [this] this))

(defn registry
  [metrics-tracer]
  (:registry metrics-tracer))

(defn metrics-tracer [] (map->MetricsTracer {}))
