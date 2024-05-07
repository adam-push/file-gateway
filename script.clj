(ns processor
  (:require [clojure.tools.nrepl.server :refer [start-server stop-server]]
            [clojure.string :as string]
            [clojure.data.json :as json]))

(defonce server (start-server :port 6156))

(defn transform
  [ctx value]
  (println "Processor: Data from file:" ctx "with value" value)
  (if-let [idx (re-find #"\d+" value)]
    {(str "processed/" ctx "/" idx) value}
    {(str "processed/" ctx) value}))
