(ns common-pg-queue.producer
  (:require [com.stuartsierra.component :as component]
            [common-pg-queue.protocols :as protocols]
            [parenthesin.helpers.logs :as logs]
            [proletarian.job :as job]))

(defrecord Producer [database]
  component/Lifecycle
  (start [this]
    (logs/log :info :producer :msg "Starting pg-queue producer...")
    this)

  (stop [this] this)

  protocols/JobProducer
  (enqueue! [_ job-type payload]
    (job/enqueue! (:datasource database) job-type payload)))

(defn new-producer []
  (map->Producer {}))
