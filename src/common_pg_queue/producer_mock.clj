(ns common-pg-queue.producer-mock
  (:require [com.stuartsierra.component :as component]
            [parenthesin.helpers.logs :as logs]))

(defprotocol MockJobProducer
  (enqueue! [self job-type payload]
    "Simula o enfileiramento de um job do tipo especificado.")
  (get-enqueued-jobs [self job-type]
    "Retorna todos os jobs enfileirados de um tipo especifico.")
  (get-all-enqueued-jobs [self]
    "Retorna todos os jobs enfileirados organizados por tipo.")
  (clear-enqueued-jobs [self]
    "Limpa todos os jobs enfileirados.")
  (has-enqueued-job? [self job-type payload]
    "Verifica se um job especifico foi enfileirado pra um tipo.")
  (get-enqueued-jobs-count [self job-type]
    "Retorna o numero de jobs enfileirados de um tipo especifico."))

(defrecord ProducerMock []
  component/Lifecycle
  (start [this]
    (logs/log :info :producer-mock :msg "Starting pg-queue producer mock...")
    (assoc this :enqueued-jobs (atom {})))

  (stop [this]
    (logs/log :info :producer-mock :msg "Stopping pg-queue producer mock...")
    (assoc this :enqueued-jobs nil))

  MockJobProducer
  (enqueue! [this job-type payload]
    (let [job-record {:payload payload
                      :job-type job-type
                      :timestamp (java.time.Instant/now)}]
      (swap! (:enqueued-jobs this) update job-type
             (fn [jobs] (conj (or jobs []) job-record)))
      (logs/log :info :producer-mock
                :msg "Mock job enqueued"
                :job-type job-type
                :payload payload)
      :enqueued))

  (get-enqueued-jobs [this job-type]
    (get @(:enqueued-jobs this) job-type []))

  (get-all-enqueued-jobs [this]
    @(:enqueued-jobs this))

  (clear-enqueued-jobs [this]
    (reset! (:enqueued-jobs this) {})
    (logs/log :info :producer-mock :msg "All enqueued jobs cleared"))

  (has-enqueued-job? [this job-type payload]
    (let [jobs (get @(:enqueued-jobs this) job-type [])]
      (boolean (some #(= payload (:payload %)) jobs))))

  (get-enqueued-jobs-count [this job-type]
    (count (get @(:enqueued-jobs this) job-type []))))

(defn new-producer-mock
  "Cria um producer mock pra testes, sem tocar em Postgres de verdade.

   Exemplo:
   (let [producer (component/start (new-producer-mock))]
     (producer-mock/enqueue! producer ::send-email {:to \"a@b.com\"})
     (producer-mock/get-enqueued-jobs producer ::send-email))"
  []
  (map->ProducerMock {}))
