(ns jepsen.elasticsearch
  (:require [clj-http.client :as http]
            [cheshire.core :as json])
  (:use [jepsen.util]
        [jepsen.set-app]
        [jepsen.load]))

(defn es-app
  [opts]
  (let [host (str "http://" (:host opts) ":9200/")
        idx "jepsen"]
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
        (:status (http/post (str host idx "/doc/" element)
                            {:body (json/encode {:body element})
                             :query-params {"refresh" true}
                             :as :string
                             :throw-exceptions true}))
        ok)

      (results [app]
        (->> (http/post (str host idx "/doc/_search")
                        {:body (json/encode {:query {:match_all {}}
                                             :size 1111
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
