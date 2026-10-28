(ns tenon.workflow.sqlite.next-jdbc
  "Lets tests run next.jdbc statements straight on a pool, each on a
   connection of it. The library itself does not need next.jdbc."
  (:require [next.jdbc.protocols :as p]
            [tenon.workflow.sqlite.pool :as pool])
  (:import [tenon.workflow.sqlite.pool Pool]))

(extend-protocol p/Executable
  Pool
  (-execute [_ _ _]
    (throw (UnsupportedOperationException. "plan needs a connection")))
  (-execute-one [this sql-params opts]
    (pool/with-connection this #(p/-execute-one % sql-params opts)))
  (-execute-all [this sql-params opts]
    (pool/with-connection this #(p/-execute-all % sql-params opts))))
