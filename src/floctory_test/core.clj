(ns floctory-test.core
	(:require [org.httpkit.server :as srv]
						[org.httpkit.client :as cln]
						[ring.middleware.params :as prm]
						[clojure.xml :as xml]
						[clojure.zip :as zip]
						[clojure.data.zip.xml :as z-x]
						[clojure.data.json :as json]))

;; The number of simultanious connections to yandex
(defonce sem (java.util.concurrent.Semaphore. 4 false))

;; Make request to yandex 
(defn search [text]
	(let [{:keys [status headers body error] :as resp} @(cln/get "http://blogs.yandex.ru/search.rss" {:query-params {:text text}})]
  (if error
    (do (println "Failed, exception: " error) '"<error/>")
    body)))

;; Make request using semaphore
(defn sensible-search [text] 
	(try
		(.acquire sem)
		(search text)
		(finally (.release sem))))

;; for every query from lst make future request to yandex and wait the results
(defn parallel-search [lst]
	(map deref
		(pmap (fn [text] (future (sensible-search text))) lst)))

;; parse xml content for links
(defn parse-links [xml]
	(let [data (zip/xml-zip 
      (xml/parse (java.io.ByteArrayInputStream. (.getBytes xml))))]
		(z-x/xml-> data :rss :channel :item :link z-x/text)
	))

;; extract queries from request, decode if needed
(defn extract-queries [lst]
	(let [queries 
					(filter #(= (first %) "query") 
						(filter #(> (count %) 1) lst))]
		(if (> (count queries) 0)
			(second (first queries))
			'[])))

;; extract domain from url, www is cropping
(defn extract-domain [url]
	(last (re-find #"^(http[s]?://|)(www[.]|)([^/:?]*).*$" url)))

;; count domains from list, result is map {domain: <number>}
(defn aggregate [rez domains]
	(reduce #(assoc %1 %2 (inc (%1 %2 0))) rez domains))

;; make alltogether
(defn search-and-aggregate [lst]
	(reduce aggregate {} 
		(map (fn [xml] 
			(map extract-domain (parse-links xml))) (parallel-search lst))))

(defn generate-error [] 
	(json/write-str {:error true :message "use /search?query=..."}))

(defn app [req]
  {:status  200
   :headers {"Content-Type" "application/json; charset=utf-8"}
   :body    (if (= (get req :uri) "/search")
							(json/write-str
								(search-and-aggregate
									(extract-queries
										(:params (prm/assoc-query-params req (get req :character-encoding))))))
							(generate-error))})


(defn -main []
 	(println "Ready.")
	(srv/run-server app {:port 8080}))

