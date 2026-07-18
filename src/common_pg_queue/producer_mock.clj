(ns common-pg-queue.producer-mock
  (:require [com.stuartsierra.component :as component]
            [common-pg-queue.protocols :as protocols]
            [parenthesin.helpers.logs :as logs]))

(defrecord ProducerMock []
  component/Lifecycle
  (start [this]
    (logs/log :info :producer-mock :msg "Starting pg-queue producer mock...")
    (assoc this :enqueued-jobs (atom {})))

  (stop [this]
    (logs/log :info :producer-mock :msg "Stopping pg-queue producer mock...")
    (assoc this :enqueued-jobs nil))

  protocols/JobProducer
  (enqueue! [this job-type payload]
    (let [job-record {:payload   payload
                      :job-type  job-type
                      :timestamp (java.time.Instant/now)}]
      (swap! (:enqueued-jobs this) update job-type
             (fn [jobs] (conj (or jobs []) job-record)))
      (logs/log :info :producer-mock
                :msg "Mock job enqueued"
                :job-type job-type
                :payload payload)
      :enqueued))

  protocols/MockJobProducer
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
  "Creates a producer mock for tests, without touching a real Postgres.
   Implements the same protocols/JobProducer as the real Producer, so
   it's a drop-in replacement wherever protocols/enqueue! is called.

   Example:
   (let [producer (component/start (new-producer-mock))]
     (protocols/enqueue! producer ::send-email {:to \"a@b.com\"})
     (protocols/get-enqueued-jobs producer ::send-email))"
  []
  (map->ProducerMock {}))
