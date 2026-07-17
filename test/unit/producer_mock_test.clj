(ns unit.producer-mock-test
  (:require [clojure.test :refer [deftest testing is]]
            [com.stuartsierra.component :as component]
            [common-pg-queue.producer-mock :as producer-mock]))

(deftest producer-mock-lifecycle-test
  (testing "should start and stop cleanly"
    (let [producer (component/start (producer-mock/new-producer-mock))]
      (is (some? (:enqueued-jobs producer)))
      (is (nil? (:enqueued-jobs (component/stop producer)))))))

(deftest producer-mock-enqueue-test
  (testing "should track enqueued jobs by job-type"
    (let [producer (component/start (producer-mock/new-producer-mock))]
      (producer-mock/enqueue! producer ::send-email {:to "a@b.com"})
      (producer-mock/enqueue! producer ::send-email {:to "c@d.com"})
      (producer-mock/enqueue! producer ::resize-image {:path "/tmp/x.png"})

      (is (= 2 (producer-mock/get-enqueued-jobs-count producer ::send-email)))
      (is (= 1 (producer-mock/get-enqueued-jobs-count producer ::resize-image)))
      (is (true? (producer-mock/has-enqueued-job? producer ::send-email {:to "a@b.com"})))
      (is (false? (producer-mock/has-enqueued-job? producer ::send-email {:to "nope@b.com"})))

      (component/stop producer))))

(deftest producer-mock-clear-test
  (testing "should clear enqueued jobs"
    (let [producer (component/start (producer-mock/new-producer-mock))]
      (producer-mock/enqueue! producer ::send-email {:to "a@b.com"})
      (producer-mock/clear-enqueued-jobs producer)

      (is (= 0 (count (producer-mock/get-all-enqueued-jobs producer))))

      (component/stop producer))))
