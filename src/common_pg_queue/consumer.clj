(ns common-pg-queue.consumer
  (:require [com.stuartsierra.component :as component]
            [parenthesin.helpers.logs :as logs]
            [proletarian.worker :as worker]))

(defrecord QueueWorker [database queue-name handler opts queue-worker]
  component/Lifecycle
  (start [this]
    (logs/log :info :queue-worker :msg "Starting queue worker..." :queue queue-name)
    (let [qw (worker/create-queue-worker
              (:datasource database)
              handler
              (merge {:proletarian/queue queue-name} opts))]
      (worker/start! qw)
      (assoc this :queue-worker qw)))

  (stop [this]
    (when queue-worker
      (logs/log :info :queue-worker :msg "Stopping queue worker..." :queue queue-name)
      (worker/stop! queue-worker))
    (assoc this :queue-worker nil)))

(defn new-queue-worker
  "Cria um worker que consome jobs de uma fila via proletarian.

   Parametros:
   - queue-name: keyword com o nome da fila (vira :proletarian/queue)
   - handler: fn/multimethod (job-type payload) - tipicamente a
     handle-job! multimethod do proletarian.job definida no projeto que
     consome esta lib
   - opts: mapa de opcoes repassado direto pro proletarian
     (:retry-strategy-fn, :failed-job-fn, :proletarian/job-table, etc.)

   Depende do component :database (component/using [:database]) - reusa o
   mesmo datasource/pool do resto da app, sem conexao propria.

   Exemplo:
   (component/using (consumer/new-queue-worker :emails handlers/handle-job!)
                    [:database])"
  ([queue-name handler]
   (new-queue-worker queue-name handler {}))
  ([queue-name handler opts]
   (map->QueueWorker {:queue-name queue-name :handler handler :opts opts})))
