(ns common-pg-queue.consumer
  (:require [com.stuartsierra.component :as component]
            [parenthesin.helpers.logs :as logs]
            [proletarian.worker :as worker]))

(defn- start-single-worker
  "Starts one proletarian queue worker based on the given config."
  [datasource {:keys [queue handler opts]}]
  (logs/log :info :queue-worker :msg "Starting queue worker..." :queue queue)
  (let [qw (worker/create-queue-worker datasource handler
                                       (merge {:proletarian/queue queue} opts))]
    (worker/start! qw)
    {:queue queue :queue-worker qw}))

(defn- stop-single-worker
  [{:keys [queue queue-worker]}]
  (logs/log :info :queue-worker :msg "Stopping queue worker..." :queue queue)
  (worker/stop! queue-worker))

(defrecord QueueWorkerManager [database workers-config active-workers]
  component/Lifecycle
  (start [this]
    (logs/log :info :queue-worker-manager :msg "Starting queue workers..."
              :count (count workers-config))
    (let [active (doall (map #(start-single-worker (:datasource database) %) workers-config))]
      (assoc this :active-workers active)))

  (stop [this]
    (logs/log :info :queue-worker-manager :msg "Stopping queue workers...")
    (doseq [w active-workers]
      (stop-single-worker w))
    (assoc this :active-workers nil)))

(defn new-queue-worker-manager
  "Creates a manager that starts one proletarian queue worker per entry in
   workers-config.

   workers-config: a list of maps, each with:
   - :queue   - keyword, queue name (becomes :proletarian/queue)
   - :handler - fn/multimethod (job-type payload), typically the
                handle-job! multimethod from proletarian.job defined in
                the project that consumes this lib
   - :opts    - (optional) map passed straight through to proletarian's
                create-queue-worker (:retry-strategy-fn, :failed-job-fn,
                :proletarian/worker-threads, etc.)

   Depends on the :database component (component/using [:database]) -
   reuses the same datasource/pool as the rest of the app, no connection
   of its own.

   Example:
   (component/using
    (consumer/new-queue-worker-manager
     [{:queue :emails :handler handlers/handle-job!}
      {:queue :thumbnails
       :handler handlers/handle-job!
       :opts {:proletarian/worker-threads 4}}])
    [:database])"
  [workers-config]
  (map->QueueWorkerManager {:workers-config workers-config}))
