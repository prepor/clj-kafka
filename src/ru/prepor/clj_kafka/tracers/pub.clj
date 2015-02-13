(ns ru.prepor.clj-kafka.tracers.pub
  (:require [ru.prepor.clj-kafka.tracer :as trace]
            [com.stuartsierra.component :as component]))

(defrecord PubTracer [tracers]
  component/Lifecycle
  (start [this] (assoc this :tracers (filter (partial satisfies? trace/Tracer) (vals this))))
  (stop [this] this)
  trace/Tracer
  (started [this group topic partition init-offset]
    (doseq [t tracers] (trace/started t group topic partition init-offset)))
  (state-changed [this group topic partition state]
    (doseq [t tracers] (trace/state-changed t group topic partition state)))
  (ack-received [this group topic partition offset]
    (doseq [t tracers] (trace/ack-received t group topic partition offset)))
  (message-received [this group topic partition offset]
    (doseq [t tracers] (trace/message-received t group topic partition offset)))
  (wait-acks-progress [this group topic partition consumed-offset acked-offset]
    (doseq [t tracers] (trace/wait-acks-progress t group topic partition
                                                 consumed-offset acked-offset)))
  (consumer-started [this group topics]
    (doseq [t tracers] (trace/consumer-started t group topics)))
  (consumer-failed [this group topics exception]
    (doseq [t tracers] (trace/consumer-failed t group topics exception)))
  (consumer-stopped [this group topics]
    (doseq [t tracers] (trace/consumer-stopped t group topics))))

(defn pub-tracer [] (map->PubTracer {}))
