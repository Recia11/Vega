(ns vega
  (:require [oz.core :as oz]
            [clojure.data.json :as json]
            [clj-http.client :as client]
            [next.jdbc :as jdbc]
            [next.jdbc.date-time]
            [clojure.core :as c]))

(def auth-url "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=DPI81RV8MDEICUBQ")
;config file
(def db-config
  {:dbtype "postgresql"
   :dbname "stockdata"
   :host "localhost"
   :user "reciaroopnarine"
   :password ""})

;error handling for url failure
(defn try-get
  [url]
  (println "Fetching " url)
  (try (client/get url)
       (catch Exception e
         {:status :exception
          :body (.getMessage e)})))

(defonce memo-try-get (memoize try-get))

(defn get-data
  [url]
  (let [response (memo-try-get url)
        {:keys [status body]} response]
    (case status
      200 (json/read-str body)
      ;should retry if get a non-200 error, after max retry - error message should be descriptive or in log
      "Non-200 error")))

(def api-response (get-data auth-url))
(def time-series (get api-response "Time Series (5min)"))
(def updated-ts (update-keys (get api-response "Time Series (5min)") #(clojure.instant/read-instant-timestamp (clojure.string/replace % #" " "T"))))
(def db (jdbc/get-datasource db-config))

(defn write-to-db [time-series db]
  (map (fn [item] (let [symbol "IBM"
                        date (key item)
                        close (BigDecimal. (get (val item) "4. close"))]
                    (jdbc/execute! db ["insert into stockdatatable (symbol,date,close)
  values(?, ? , ?)" symbol date close]))) time-series))

(defn get-data-for-chart [updated-ts]
  (map (fn [item] {:item "IBM" :time (key item) :quantity (BigDecimal. (get (val item) "4. close"))}) updated-ts))


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

(def line-plot-stock
  {:data {:values (get-data-for-chart updated-ts)}
   :encoding {:x {:field "time" :type "temporal"}
              :y {:field "quantity" :type "quantitative"}}

   :mark "line"})

;; Render the plot
;(oz/view! line-plot)
;(oz/view! line-plot-stock)

;(def contour-plot (oz/load "examples/contour-lines.vega.json"))
;(oz/view! contour-plot :mode :vega)

(def viz
  [:div
   [:h1 "Stock Data"]
   [:p "IBM"]
   [:div {:style {:display "flex" :flex-direction "row"}}
    [:vega-lite line-plot]]

   [:p "Tesla"]
   [:vega stacked-bar]
   [:h2 "The trend is showing that IBM stock price has gone up 400% over the last 10 years"]
   [:p "But there is no guarantee that it will continue to rise"]])

;(oz/view! viz)