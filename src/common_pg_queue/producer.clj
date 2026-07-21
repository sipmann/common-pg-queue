(ns common-pg-queue.producer
  (:require [com.stuartsierra.component :as component]
            [common-pg-queue.protocols :as protocols]
            [parenthesin.helpers.logs :as logs]
            [proletarian.job :as job])
  (:import [javax.sql DataSource]))

(defrecord Producer [database]
  component/Lifecycle
  (start [this]
    (logs/log :info :producer :msg "Starting pg-queue producer...")
    this)

  (stop [this] this)

  protocols/JobProducer
  ;; proletarian.job/enqueue! quer um java.sql.Connection de verdade
  ;; (assert (instance? Connection conn)) - (:datasource database) e um
  ;; javax.sql.DataSource (o pool do HikariCP), nao uma Connection, entao
  ;; abrimos uma conexao do pool aqui antes de enfileirar
  (enqueue! [_ job-type payload]
    (with-open [conn (.getConnection ^DataSource (:datasource database))]
      (job/enqueue! conn job-type payload)))

  (enqueue! [_ job-type payload opts]
    (with-open [conn (.getConnection ^DataSource (:datasource database))]
      (apply job/enqueue! conn job-type payload (apply concat opts)))))

(defn new-producer []
  (map->Producer {}))
