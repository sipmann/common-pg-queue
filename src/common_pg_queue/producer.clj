(ns common-pg-queue.producer
  (:require [com.stuartsierra.component :as component]
            [parenthesin.helpers.logs :as logs]
            [proletarian.job :as job]))

(defprotocol JobProducer
  (enqueue! [self job-type payload]
    "Enqueues a job for asynchronous processing. job-type is a keyword,
    payload is a map. Runs on the same datasource/pool as the rest of the
    app - no connection of its own."))

(defrecord Producer [database]
  component/Lifecycle
  (start [this]
    (logs/log :info :producer :msg "Starting pg-queue producer...")
    this)

  (stop [this] this)

  JobProducer
  (enqueue! [this job-type payload]
    (job/enqueue! (:datasource database) job-type payload)))

(defn new-producer []
  (map->Producer {}))
