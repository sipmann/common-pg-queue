(ns unit.consumer-mock-test
  (:require [clojure.test :refer [deftest testing is]]
            [com.stuartsierra.component :as component]
            [common-pg-queue.consumer-mock :as consumer-mock]))

(defmulti test-handler (fn [job-type _payload] job-type))

(defmethod test-handler ::send-email
  [_job-type {:keys [to]}]
  (str "sent to " to))

(defmethod test-handler ::always-fails
  [_job-type _payload]
  (throw (ex-info "boom" {})))

(deftest consumer-mock-lifecycle-test
  (testing "should start and stop cleanly"
    (let [worker (component/start (consumer-mock/new-queue-worker-mock test-handler))]
      (is (some? (:processed-jobs worker)))
      (is (nil? (:processed-jobs (component/stop worker)))))))

(deftest consumer-mock-processes-job-test
  (testing "should call the handler and record the outcome"
    (let [worker (component/start (consumer-mock/new-queue-worker-mock test-handler))]
      (is (= :done (consumer-mock/consume-job! worker ::send-email {:to "a@b.com"})))

      (let [[job] (consumer-mock/get-processed-jobs worker ::send-email)]
        (is (= {:to "a@b.com"} (:payload job)))
        (is (= :done (:outcome job)))
        (is (= "sent to a@b.com" (:result job))))

      (component/stop worker))))

(deftest consumer-mock-handles-failure-test
  (testing "should mark the job as failed instead of throwing"
    (let [worker (component/start (consumer-mock/new-queue-worker-mock test-handler))]
      (is (= :failed (consumer-mock/consume-job! worker ::always-fails {})))

      (let [[job] (consumer-mock/get-processed-jobs worker ::always-fails)]
        (is (= :failed (:outcome job)))
        (is (some? (:error job))))

      (component/stop worker))))

(deftest consumer-mock-clear-test
  (testing "should clear processed jobs"
    (let [worker (component/start (consumer-mock/new-queue-worker-mock test-handler))]
      (consumer-mock/consume-job! worker ::send-email {:to "a@b.com"})
      (consumer-mock/clear-processed-jobs worker)

      (is (= 0 (count (consumer-mock/get-all-processed-jobs worker))))

      (component/stop worker))))
