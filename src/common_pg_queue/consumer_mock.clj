(ns common-pg-queue.consumer-mock
  (:require [com.stuartsierra.component :as component]
            [common-pg-queue.protocols :as protocols]
            [parenthesin.helpers.logs :as logs]))

(defn- start-single-mock-worker
  [{:keys [queue handler]}]
  {:queue          queue
   :handler        handler
   :processed-jobs (atom [])})

(defrecord QueueWorkerManagerMock [workers-config active-workers]
  component/Lifecycle
  (start [this]
    (logs/log :info :queue-worker-manager-mock :msg "Starting queue worker mocks..."
              :count (count workers-config))
    (assoc this :active-workers (doall (map start-single-mock-worker workers-config))))

  (stop [this]
    (logs/log :info :queue-worker-manager-mock :msg "Stopping queue worker mocks...")
    (assoc this :active-workers nil))

  protocols/MockQueueWorkerManager
  (consume-job! [_ queue job-type payload]
    (if-let [worker (first (filter #(= (:queue %) queue) active-workers))]
      (let [handler        (:handler worker)
            [result error] (try
                            [(handler job-type payload) nil]
                            (catch Exception e
                              [nil e]))
            outcome        (if error :failed :done)]
        (swap! (:processed-jobs worker) conj
               {:job-type job-type
                :payload payload
                :outcome outcome
                :result result
                :error error
                :timestamp (java.time.Instant/now)})
        (logs/log (if error :error :info) :queue-worker-manager-mock
                  :msg "Mock job processed"
                  :queue queue
                  :job-type job-type
                  :outcome outcome)
        outcome)
      (do
        (logs/log :warn :queue-worker-manager-mock :msg "No worker found for queue" :queue queue)
        :no-worker-found)))

  (get-processed-jobs [_ queue]
                      (if-let [worker (first (filter #(= (:queue %) queue) active-workers))]
                        @(:processed-jobs worker)
                        []))

  (get-all-processed-jobs [_]
    (reduce (fn [acc worker] (assoc acc (:queue worker) @(:processed-jobs worker)))
            {}
            active-workers))

  (clear-processed-jobs [_]
    (doseq [worker active-workers]
      (reset! (:processed-jobs worker) []))
    (logs/log :info :queue-worker-manager-mock :msg "All processed jobs cleared")))

(defn new-queue-worker-manager-mock
  "Creates a worker manager mock for tests - same workers-config shape as
   the real manager, but processes jobs immediately, without a
   queue/Postgres.

   Example:
   (let [manager (component/start
                  (new-queue-worker-manager-mock
                   [{:queue :emails :handler handlers/handle-job!}]))]
     (protocols/consume-job! manager :emails ::send-email {:to \"a@b.com\"})
     (protocols/get-processed-jobs manager :emails))"
  [workers-config]
  (map->QueueWorkerManagerMock {:workers-config workers-config}))
