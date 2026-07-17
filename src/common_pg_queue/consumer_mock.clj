(ns common-pg-queue.consumer-mock
  (:require [com.stuartsierra.component :as component]
            [parenthesin.helpers.logs :as logs]))

(defprotocol MockQueueWorker
  (consume-job! [self job-type payload]
    "Simula o processamento sincrono de um job, chamando o handler
    diretamente (sem Postgres/proletarian de verdade).")
  (get-processed-jobs [self job-type]
    "Retorna todos os jobs processados de um tipo especifico.")
  (get-all-processed-jobs [self]
    "Retorna todos os jobs processados organizados por tipo.")
  (clear-processed-jobs [self]
    "Limpa todos os jobs processados."))

(defrecord QueueWorkerMock [handler processed-jobs]
  component/Lifecycle
  (start [this]
    (logs/log :info :queue-worker-mock :msg "Starting pg-queue consumer mock...")
    (assoc this :processed-jobs (atom {})))

  (stop [this]
    (logs/log :info :queue-worker-mock :msg "Stopping pg-queue consumer mock...")
    (assoc this :processed-jobs nil))

  MockQueueWorker
  (consume-job! [this job-type payload]
    (let [[result error] (try
                           [(handler job-type payload) nil]
                           (catch Exception e
                             [nil e]))
          outcome (if error :failed :done)]
      (swap! (:processed-jobs this) update job-type
             (fn [jobs] (conj (or jobs [])
                              {:payload payload
                               :outcome outcome
                               :result result
                               :error error
                               :timestamp (java.time.Instant/now)})))
      (logs/log (if error :error :info) :queue-worker-mock
                :msg "Mock job processed"
                :job-type job-type
                :outcome outcome)
      outcome))

  (get-processed-jobs [this job-type]
    (get @(:processed-jobs this) job-type []))

  (get-all-processed-jobs [this]
    @(:processed-jobs this))

  (clear-processed-jobs [this]
    (reset! (:processed-jobs this) {})
    (logs/log :info :queue-worker-mock :msg "All processed jobs cleared")))

(defn new-queue-worker-mock
  "Cria um worker mock pra testes - mesma assinatura de handler
   (job-type payload) do worker de verdade, mas processa na hora, sem
   fila/Postgres.

   Exemplo:
   (let [worker (component/start (new-queue-worker-mock handlers/handle-job!))]
     (consumer-mock/consume-job! worker ::send-email {:to \"a@b.com\"})
     (consumer-mock/get-processed-jobs worker ::send-email))"
  [handler]
  (map->QueueWorkerMock {:handler handler}))
