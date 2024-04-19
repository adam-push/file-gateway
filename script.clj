(ns processor
  (:require [clojure.tools.nrepl.server :refer [start-server stop-server]]
            [clojure.string :as string]
            [clojure.data.json :as json]))

(defonce server (start-server :port 6156))

(defn transform
  [ctx value]
  (println "++AST: from file:" ctx "with value" value)
  {(str "processed/" ctx) value})
