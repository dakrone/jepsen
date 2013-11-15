(ns jepsen.elasticsearch
  (:require [clj-http.client :as http]
            [cheshire.core :as json]
            [jepsen.failure :as failure])
  (:use [jepsen.util]
        [jepsen.set-app]
        [jepsen.load]))

(defn es-app
  [opts]
  (let [host (str "http://" (:host opts) ":9200/")
        idx "jepsen"
        type "doc"]
    (reify SetApp
      (setup [app]
        (teardown app)
        (-> (http/post
             (str host idx)
             {:body
              (json/encode {:settings {"number_of_shards" 5
                                       "number_of_replicas" 1}})
              :as :json})
            :body))

      (add [app element]
        (:status (http/post (str host idx "/" type "/" element)
                            {:body (json/encode {:body element})
                             :as :string
                             :throw-exceptions true}))
        ok)

      (results [app]
        (http/post (str host idx "/_refresh"))
        (->> (http/post (str host idx "/" type "/_search")
                        {:body (json/encode {:query {:match_all {}}
                                             :size 2000
                                             :from 0})
                         :as :json})
             :body
             :hits
             :hits
             (mapcat :_source)
             (map second)
             (map long)
             set))

      (teardown [app]
        (:body (http/delete (str host idx)
                            {:throw-exceptions false
                             :as :json}))))))

(defn es-update-app
  [opts]
  (let [host (str "http://" (:host opts) ":9200/")
        idx "jepsen2"
        type "doc"]
    (reify SetApp
      (setup [app]
        ;; Delete old index
        (teardown app)
        ;; Create the index
        (http/post
         (str host idx)
         {:body
          (json/encode {:settings {"number_of_shards" 5
                                   "number_of_replicas" 1}})
          :as :json})
        ;; Index the starting doc
        (-> (http/post (str host idx "/" type "/mydoc")
                       {:body (json/encode {:body []})
                        :as :json
                        :throw-exceptions true})
            :body))

      (add [app element]
        (:status (http/post (str host idx "/" type "/mydoc/_update")
                            {:body (json/encode
                                    {:script "ctx._source.body += id"
                                     :params {:id element}})
                             :as :string
                             :throw-exceptions true}))
        ok)

      (results [app]
        (http/post (str host idx "/_refresh"))
        (->> (http/post (str host idx "/" type "/_search")
                        {:body (json/encode {:query {:match_all {}}
                                             :size 1
                                             :from 0})
                         :as :json})
             :body
             :hits
             :hits
             first
             :_source
             (into {})
             :body
             set))

      (teardown [app]
        (:body (http/delete (str host idx)
                            {:throw-exceptions false
                             :as :json}))))))

(def failure
  (let [leader (atom nil)]
    (reify failure/Failure

      (fail [_ nodes]
        ;; TODO: determine master node and make sure it's partitioned
        )

      (recover [_ nodes]
        ;; TODO: figure out what we should do with recovering
        ))))
