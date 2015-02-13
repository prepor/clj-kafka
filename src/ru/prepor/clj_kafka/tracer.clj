(ns ru.prepor.clj-kafka.tracer)

(defprotocol Tracer
  (started [this group topic partition init-offset])
  ;; try-to-lock cant-lock started wait-acks completed
  (state-changed [this group topic partition state])
  (ack-received [this group topic partition offset])
  (message-received [this group topic partition offset])
  (wait-acks-progress [this group topic partition consumed-offset acked-offset])
  (consumer-started [this  group topics])
  (consumer-failed [this group topics exception])
  (consumer-stopped [this group topics]))

(defrecord NilTracer []
  Tracer
  (started [this group topic partition init-offset])
  (state-changed [this group topic partition state])
  (ack-received [this group topic partition offset])
  (message-received [this group topic partition offset])
  (wait-acks-progress [this group topic partition consumed-offset acked-offset])
  (consumer-started [this topics group])
  (consumer-failed [this topics group exception])
  (consumer-stopped [this topics group]))

(defn nil-tracer [] (map->NilTracer {}))
