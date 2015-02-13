(ns ru.prepor.clj-kafka.constant)

;; (defn consumer
;;   "The most primitive implementation of distributive client. Based on total-n and
;;   current-n configuration, without any automatic rebalancing.
;;   Requests are executed in go-blocks thread-pool, with blocking IO, but without _wait-for_
;;   blocking."
;;   [kafka {:keys [group topics total-n current-n init-offsets] :or {init-offsets :latest}}]
;;   (let [m (topics-metadata kafka topics)
;;         channels (a/chan)
;;         control-ch (a/chan)
;;         threads (for [t m
;;                       p (:partition-metadata t)
;;                       :when (= current-n (mod (:id p) total-n))]
;;                   (a/>! channels {:topic topic :partition (:id partition) :init-offset init-offset
;;                                   :chan ch})
;;                   (constant-partition kafka t p channels control-ch
;;                                       {:group group
;;                                        :init-offsets init-offsets}))
;;         stop! (fn [] (a/close! control-ch) (a/close! channels))]
;;     (doall threads)
;;     ;; Closes all channels on first failure
;;     (let [threads-ch (a/merge threads)]
;;       (a/go-loop []
;;         (when-let [v (a/<! threads-ch)]
;;           (when (utils/throwable? v)
;;             (log/errorf v "Stop kafka consumer %s" topics)
;;             (stop!))
;;           (recur))))
;;     [stop! channels]))
