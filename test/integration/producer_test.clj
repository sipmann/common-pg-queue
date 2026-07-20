(ns integration.producer-test
  "Integration test against a real (embedded) Postgres - fills in the
   'Integration tests against embedded Postgres' item from the README TODO.
   This is the test that would have caught the enqueue! bug: producer.clj
   used to pass the HikariCP DataSource straight to proletarian.job/enqueue!,
   which asserts its first argument is a java.sql.Connection."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [com.stuartsierra.component :as component]
            [common-pg-queue.producer :as producer]
            [common-pg-queue.protocols :as protocols]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [pg-embedded-clj.core :as pg-emb]))

(def ^:private db-port 28432)

(defn- test-datasource []
  (jdbc/get-datasource {:dbtype "postgres" :dbname "postgres" :host "localhost"
                        :port db-port :user "postgres" :password "postgres"}))

;; DDL copiado de database/postgresql/tables.sql - executado em statements
;; separados porque next.jdbc/execute! roda um comando por vez
(defn- install-proletarian-tables! [ds]
  (jdbc/execute! ds ["CREATE SCHEMA IF NOT EXISTS proletarian"])
  (jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS proletarian.job
                      (job_id UUID PRIMARY KEY,
                       queue TEXT NOT NULL,
                       job_type TEXT NOT NULL,
                       payload TEXT NOT NULL,
                       attempts INTEGER NOT NULL,
                       enqueued_at TIMESTAMPTZ NOT NULL,
                       process_at TIMESTAMPTZ NOT NULL)"])
  (jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS proletarian.archived_job
                      (job_id UUID PRIMARY KEY,
                       queue TEXT NOT NULL,
                       job_type TEXT NOT NULL,
                       payload TEXT NOT NULL,
                       attempts INTEGER NOT NULL,
                       enqueued_at TIMESTAMPTZ NOT NULL,
                       process_at TIMESTAMPTZ NOT NULL,
                       status TEXT NOT NULL,
                       finished_at TIMESTAMPTZ NOT NULL)"])
  (jdbc/execute! ds ["DROP INDEX IF EXISTS proletarian.job_queue_process_at"])
  (jdbc/execute! ds ["CREATE INDEX job_queue_process_at ON proletarian.job (queue, process_at)"]))

(use-fixtures :once
  (fn [f]
    (pg-emb/init-pg {:port db-port})
    (install-proletarian-tables! (test-datasource))
    (f)
    (pg-emb/halt-pg!)))

(deftest enqueue-writes-a-real-job-row-test
  (testing "enqueue! opens a real Connection from the DataSource before handing it to proletarian.job/enqueue!"
    (let [ds (test-datasource)
          producer (-> (producer/new-producer)
                      component/start
                      (assoc :database {:datasource ds}))]
      (protocols/enqueue! producer :test-job {:hello "world"})
      (let [rows (jdbc/execute! ds ["SELECT job_type, payload FROM proletarian.job"]
                                {:builder-fn rs/as-unqualified-maps})]
        (is (= 1 (count rows)))
        (is (= ":test-job" (:job_type (first rows))))
        (is (re-find #"hello" (:payload (first rows))))))))
