(ns ru.prepor.clj-kafka.tracers.pub
  (:require [defcomponent :refer [defcomponent]]
            [ru.prepor.clj-kafka.tracer :as trace]))

(defcomponent pub-tracer []
  []
  (start [this] (assoc this :tracers (filter (partial satisfies? trace/Tracer) (vals this))))
  (stop [this] this)
  trace/Tracer
  (started [this group topic partition init-offset]
    (doseq [t (:tracers this)] (trace/started t group topic partition init-offset)))
  (state-changed [this group topic partition state]
    (doseq [t (:tracers this)] (trace/state-changed t group topic partition state)))
  (ack-received [this group topic partition offset]
    (doseq [t (:tracers this)] (trace/ack-received t group topic partition offset)))
  (message-received [this group topic partition offset]
    (doseq [t (:tracers this)] (trace/message-received t group topic partition offset)))
  (wait-acks-progress [this group topic partition consumed-offset acked-offset]
    (doseq [t (:tracers this)] (trace/wait-acks-progress t group topic partition
                                                 consumed-offset acked-offset)))
  (consumer-started [this group topics]
    (doseq [t (:tracers this)] (trace/consumer-started t group topics)))
  (consumer-failed [this group topics exception]
    (doseq [t (:tracers this)] (trace/consumer-failed t group topics exception)))
  (consumer-stopped [this group topics]
    (doseq [t (:tracers this)] (trace/consumer-stopped t group topics))))
