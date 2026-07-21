(ns common-pg-queue.protocols
  "Protocol definitions for common-pg-queue. Kept separate from the
  components that implement them so producer.clj and producer-mock.clj
  can both satisfy the same JobProducer protocol - callers depend on
  these vars, not on which namespace built the record, so a real
  Producer and a ProducerMock are truly interchangeable.")

(defprotocol JobProducer
  (enqueue!
    [self job-type payload]
    [self job-type payload opts]
    "Enqueues a job for asynchronous processing. job-type is a keyword,
    payload is a map. opts is an optional map of proletarian enqueue
    options (:proletarian/queue, :process-at, :process-in, etc.),
    passed straight through to proletarian.job/enqueue! - defaults to
    the :proletarian/default queue when omitted."))

(defprotocol MockJobProducer
  (get-enqueued-jobs [self job-type]
    "Returns all enqueued jobs for a specific type.")
  (get-all-enqueued-jobs [self]
    "Returns all enqueued jobs organized by type.")
  (clear-enqueued-jobs [self]
    "Clears all enqueued jobs.")
  (has-enqueued-job? [self job-type payload]
    "Checks whether a specific job was enqueued for a type.")
  (get-enqueued-jobs-count [self job-type]
    "Returns the number of enqueued jobs for a specific type."))

(defprotocol MockQueueWorkerManager
  (consume-job! [self queue job-type payload]
    "Simulates synchronous processing of a job on the given queue, calling
    that queue's handler directly (without a real Postgres/proletarian).")
  (get-processed-jobs [self queue]
    "Returns all jobs processed on a specific queue.")
  (get-all-processed-jobs [self]
    "Returns all processed jobs organized by queue.")
  (clear-processed-jobs [self]
    "Clears all processed jobs."))
