(ns vega
  (:require [oz.core :as oz]
            [clojure.data.json :as json]
            [clj-http.client :as client]
            [next.jdbc :as jdbc]
            [next.jdbc.date-time]
            [clojure.core :as c]))

;config file
(def db-config
  {:dbtype "postgresql"
   :dbname "stockdata"
   :host "localhost"
   :user "reciaroopnarine"
   :password ""})

(def db (jdbc/get-datasource db-config))

(defn write-to-db [time-series db]
  (map (fn [item] (let [symbol "IBM"
                        date (key item)
                        close (BigDecimal. (get (val item) "4. close"))]
                    (jdbc/execute! db ["insert into stockdatatable (symbol,date,close)
  values(?, ? , ?)" symbol date close]))) time-series))

;;;;;;;;;;

#_(def api-response (get-data auth-url))
#_(def time-series (get api-response "Time Series (5min)"))
#_(def updated-ts (update-keys (get api-response "Time Series (5min)")
                             #(clojure.instant/read-instant-timestamp (clojure.string/replace % #" " "T"))))

(def auth-url "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=DPI81RV8MDEICUBQ")

;error handling for url failure
(defn try-get
  [url]
  (println "Fetching " url)
  (try (client/get url)
       (catch Exception e
         {:status :exception
          :body (.getMessage e)})))

(defn get-data
  [url]
  (let [response (try-get url)
        {:keys [status body]} response]
    (case status
      200 (json/read-str body)
      ;should retry if get a non-200 error, after max retry - error message should be descriptive or in log
      "Non-200 error")))

(defn get-data-intraday
  [symbol interval]
  (let [url (str "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY" "&symbol=" symbol
                 "&interval=" interval "&apikey=DPI81RV8MDEICUBQ")]
    (get-data url)))

(defn get-data-weekly
  [symbol]
  (get-data (str "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol="
                 symbol "&apikey=DPI81RV8MDEICUBQ")))

(def ibm-weekly (-> (slurp "data/ibm-weekly.edn") clojure.edn/read-string))
(def tsla-weekly (-> (slurp "data/tsla-weekly.edn") clojure.edn/read-string))

(defn response->vega-data-weekly
  [response-edn stock-ticker]
  (let [time-series (-> (get response-edn "Weekly Time Series")
                        (update-keys #(clojure.instant/read-instant-timestamp (clojure.string/replace % #" " "T")))
                        (update-vals #(Double/parseDouble (get % "4. close"))))]
    (map (fn [[k v]] {:stock-ticker stock-ticker :timestamp k :close v}) time-series)))

(def stock-price-line-plot
  {:data     {:values
              (concat (response->vega-data-weekly ibm-weekly "IBM")
                      (response->vega-data-weekly tsla-weekly "TSLA"))}
   :encoding {:x     {:field "timestamp" :type "temporal" :width 600}
              :y     {:field "close" :type "quantitative"}
              :color {:field "stock-ticker" :type "nominal"}}
   :mark     "line"})

(oz/view! stock-price-line-plot)

;(oz/view! line-plot-stock)

;;;;;;;;; Play data

(defn play-data [& names]
  (for [n names
        i (range 20)]
    {:time i :item n :quantity (+ (Math/pow (* i (count n)) 0.8) (rand-int (count n)))}))

(def stacked-bar
  {:data     {:values (play-data "munchkin" "witch" "dog" "lion" "tiger" "bear")}
   :mark     "bar"
   :encoding {:x     {:field "time"}
              :y     {:aggregate "sum"
                      :field     "quantity"
                      :type      "quantitative"}
              :color {:field "item"}}})

;(oz/start-server!)

(def line-plot
  {:data {:values (play-data "monkey" "slipper" "broom")}
   :encoding {:x {:field "time" :type "quantitative"}
              :y {:field "quantity" :type "quantitative"}
              :color {:field "item" :type "nominal"}}
   :mark "line"})

;; Render the plot
;(oz/view! line-plot)

;(def contour-plot (oz/load "examples/contour-lines.vega.json"))
;(oz/view! contour-plot :mode :vega)

(def viz
  [:div
   [:h1 "IBM vs TSLA"]
   [:p "TSLA started way below IBM's stock price, then shot up into the stratosphere in the begining of 2020, and
   currently trades slightly above IBM."]
   [:div {:style {:display "flex" :flex-direction "row"}}
    [:vega-lite stock-price-line-plot {:width 600 :height 400}]]])

(oz/view! viz)