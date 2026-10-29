(ns hooks.kerodon)

(defmacro within
  "kerodon.core/within threads state through fns, scoped to selector."
  [state selector & fns]
  `(do ~selector (-> ~state ~@fns)))
