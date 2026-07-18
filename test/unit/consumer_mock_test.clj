(ns unit.consumer-mock-test
  (:require [clojure.test :refer [deftest testing is]]
            [com.stuartsierra.component :as component]
            [common-pg-queue.consumer-mock :as consumer-mock]
            [common-pg-queue.protocols :as protocols]))

(defmulti test-handler (fn [job-type _payload] job-type))

(defmethod test-handler ::send-email
  [_job-type {:keys [to]}]
  (str "sent to " to))

(defmethod test-handler ::always-fails
  [_job-type _payload]
  (throw (ex-info "boom" {})))

(def test-workers-config
  [{:queue :emails :handler test-handler}
   {:queue :failing-queue :handler test-handler}])

(deftest consumer-mock-lifecycle-test
  (testing "should start and stop cleanly"
    (let [manager (component/start (consumer-mock/new-queue-worker-manager-mock test-workers-config))]
      (is (some? (:active-workers manager)))
      (is (= 2 (count (:active-workers manager))))
      (is (nil? (:active-workers (component/stop manager)))))))

(deftest consumer-mock-processes-job-test
  (testing "should call the matching queue's handler and record the outcome"
    (let [manager (component/start (consumer-mock/new-queue-worker-manager-mock test-workers-config))]
      (is (= :done (protocols/consume-job! manager :emails ::send-email {:to "a@b.com"})))

      (let [[job] (protocols/get-processed-jobs manager :emails)]
        (is (= {:to "a@b.com"} (:payload job)))
        (is (= :done (:outcome job)))
        (is (= "sent to a@b.com" (:result job))))

      (component/stop manager))))

(deftest consumer-mock-handles-failure-test
  (testing "should mark the job as failed instead of throwing"
    (let [manager (component/start (consumer-mock/new-queue-worker-manager-mock test-workers-config))]
      (is (= :failed (protocols/consume-job! manager :failing-queue ::always-fails {})))

      (let [[job] (protocols/get-processed-jobs manager :failing-queue)]
        (is (= :failed (:outcome job)))
        (is (some? (:error job))))

      (component/stop manager))))

(deftest consumer-mock-unknown-queue-test
  (testing "should handle an unknown queue gracefully"
    (let [manager (component/start (consumer-mock/new-queue-worker-manager-mock test-workers-config))]
      (is (= :no-worker-found (protocols/consume-job! manager :unknown-queue ::send-email {})))
      (component/stop manager))))

(deftest consumer-mock-multiple-queues-test
  (testing "should track jobs independently per queue"
    (let [manager (component/start (consumer-mock/new-queue-worker-manager-mock test-workers-config))]
      (protocols/consume-job! manager :emails ::send-email {:to "a@b.com"})
      (protocols/consume-job! manager :emails ::send-email {:to "c@d.com"})

      (is (= 2 (count (protocols/get-processed-jobs manager :emails))))
      (is (= 0 (count (protocols/get-processed-jobs manager :failing-queue))))
      (is (contains? (protocols/get-all-processed-jobs manager) :emails))

      (component/stop manager))))

(deftest consumer-mock-clear-test
  (testing "should clear processed jobs"
    (let [manager (component/start (consumer-mock/new-queue-worker-manager-mock test-workers-config))]
      (protocols/consume-job! manager :emails ::send-email {:to "a@b.com"})
      (protocols/clear-processed-jobs manager)

      (is (= 0 (count (protocols/get-processed-jobs manager :emails))))

      (component/stop manager))))
