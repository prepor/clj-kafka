(ns ru.prepor.clj-kafka.tracers.state
  (:require [ru.prepor.clj-kafka.tracer :as trace]
            [com.stuartsierra.component :as component]))

;; we assumed that we have only one consumer per group
(defrecord StateTracer [state]
  component/Lifecycle
  (start [this] (assoc this :state (atom {})))
  (stop [this] this)
  trace/Tracer
  (started [this group topic partition init-offset]
    (swap! state assoc-in [:partitions group topic partition]
           {:initialized-from-offset init-offset
            :state :started}))
  (state-changed [this group topic partition new-state]
    (swap! state assoc-in [:partitions group topic partition :state] new-state))
  (ack-received [this group topic partition offset])
  (message-received [this group topic partition offset])
  (wait-acks-progress [this group topic partition consumed-offset acked-offset]
    (swap! state assoc-in [:partitions group topic partition :wait-acks]
           {:consumed consumed-offset
            :acked acked-offset
            :lag (- (or consumed-offset 0) (or acked-offset 0))}))
  (consumer-started [this group topics]
    (swap! state assoc-in [:consumers group] {:topics topics :state :started}))
  (consumer-failed [this group topics exception]
    (swap! state assoc-in [:consumers group :state] :failed))
  (consumer-stopped [this group topics]
    (swap! state assoc-in [:consumers group :state] :stopped)))

(defn state
  [state-tracer]
  @(:state state-tracer))

(defn state-tracer [] (map->StateTracer {}))
