(ns ru.prepor.clj-kafka.tracers.log
  (:require [clojure.tools.logging :as log]
            [ru.prepor.clj-kafka.tracer :as trace]))

(defrecord LogTracer []
  trace/Tracer
  (started [this group topic partition init-offset]
    (log/infof "%s(%s:%s) partition started with init offset: %s"
               group topic partition init-offset))
  (state-changed [this group topic partition state]
    (log/infof "%s(%s:%s) partition state changed: %s" group topic partition state))
  (message-received [this group topic partition offset]
    (log/tracef "%s(%s:%s) message received: %s" group topic partition offset))
  (ack-received [this group topic partition offset]
    (log/tracef "%s(%s:%s) ack received: %s" group topic partition offset))
  (wait-acks-progress [this group topic partition consumed-offset acked-offset]
    (log/debugf "%s(%s:%s) partition wait acks progress: consumed: %s; acked: %s"
                group topic partition consumed-offset acked-offset))
  (consumer-started [this group topics]
    (log/infof "%s(%s) consumer started" group (pr-str topics)))
  (consumer-failed [this group topics exception]
    (log/errorf exception "%s(%s) consumer failed" group (pr-str topics)))
  (consumer-stopped [this group topics]
    (log/infof "%s(%s) consumer stopped" group (pr-str topics))))

(defn log-tracer [] (map->LogTracer {}))
