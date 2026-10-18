(ns tenon.workflow.sqlite-test
  (:require [clojure.test :refer [deftest]]
            [tenon.workflow.db :as db]
            [tenon.workflow.sqlite :as sqlite]
            [tenon.workflow.storage-contract :as contract]))

(deftest sqlite-storage-contract-test
  (let [path (.getAbsolutePath (java.io.File/createTempFile "tenon-contract" ".db"))]
    (try
      (with-open [ds (sqlite/datasource path)]
        (contract/check-storage (db/init-db! ds)))
      (finally
        (doseq [suffix ["" "-wal" "-shm"]]
          (.delete (java.io.File. (str path suffix))))))))
