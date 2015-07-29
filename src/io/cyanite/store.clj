(ns io.cyanite.store
  "Implements a metric store on top of cassandra. This currently
   relies on a single schema. All cassandra interaction bits
   should quickly be abstracted with a protocol to more easily
   swap implementations"
  (:require [clojure.string        :as str]
            [qbits.alia            :as alia]
            [qbits.alia.policy.load-balancing :as lb]
            [io.cyanite.util       :refer [partition-or-time
                                           go-forever go-catch]]
            [clojure.tools.logging :refer [error info debug]]
            [clojure.core.async    :refer [take! <! >! go chan]])
  (:import [com.datastax.driver.core
            BatchStatement
            PreparedStatement]))

(set! *warn-on-reflection* true)

; Define an atom to be used for prepared statements dynamically
; generated
(def p (atom {}))

; Define an atom to prevent rollups from looping during a period
(def procrollups (atom {}))

(defprotocol Metricstore
  (insert [this ttl data tenant rollup period path time])
  (channel-for [this])
  (fetch [this agg table paths tenant rollup period from to]))

;;
;; The following contains necessary cassandra queries. Since
;; cyanite relies on very few queries, I decided against using
;; hayt

; Take a dynamic table name for creating a prepared statement in the 
; later code
(defn makeinsertquery
  "Yields a cassandra prepared statement of 6 arguments:

* `ttl`: how long to keep the point around
* `metric`: the data point
* `rollup`: interval between points at this resolution
* `period`: rollup multiplier which determines the time to keep points for
* `path`: name of the metric
* `time`: timestamp of the metric, should be divisible by rollup"
  [table]
   (str
    "UPDATE " table 
    " USING TTL ? SET data = data + ? "
    "WHERE tenant = '' AND rollup = ? AND period = ? AND path = ? AND time = ?;"))

(defn makerollupinsertquery
  "Yields a cassandra prepared statement of 6 arguments:

* `ttl`: how long to keep the point around
* `metric`: the data point
* `rollup`: interval between points at this resolution
* `period`: rollup multiplier which determines the time to keep points for
* `path`: name of the metric
* `time`: timestamp of the metric, should be divisible by rollup"
  [table]
   (str
    "UPDATE " table 
    " USING TTL ? SET data = ? "
    "WHERE tenant = '' AND rollup = ? AND period = ? AND path = ? AND time = ?;"))

; Dynamic fetching
(defn makefetchquery
  "Yields a cassandra prepared statement of 6 arguments:

* `paths`: list of paths
* `rollup`: interval between points at this resolution
* `period`: rollup multiplier which determines the time to keep points for
* `min`: return points starting from this timestamp
* `max`: return points up to this timestamp
* `limit`: maximum number of points to return"
  [table]
   (str
    "SELECT path,data,time FROM " table " WHERE path IN ? AND tenant = '' AND rollup = ? AND period = ? "
    "AND time >= ? AND time <= ? ORDER BY time ASC;"))

; Dynamic fetching
(defn makerollupfetchquery
  "Yields a cassandra prepared statement of 6 arguments:

* `paths`: list of paths
* `rollup`: interval between points at this resolution
* `period`: rollup multiplier which determines the time to keep points for
* `min`: return points starting from this timestamp
* `max`: return points up to this timestamp
* `limit`: maximum number of points to return"
  [table]
   (str
    "SELECT data FROM " table " WHERE path = ? AND tenant = '' AND rollup = ? AND period = ? "
    "AND time >= ? AND time < ? ORDER BY time ASC;"))

(defn makerollupinsertquery
  "Yields a cassandra prepared statement of 6 arguments:

* `ttl`: how long to keep the point around
* `metric`: the data point
* `rollup`: interval between points at this resolution
* `period`: rollup multiplier which determines the time to keep points for
* `path`: name of the metric
* `time`: timestamp of the metric, should be divisible by rollup"
  [table]
   (str
    "UPDATE " table
    " USING TTL ? SET data = ? "
    "WHERE tenant = '' AND rollup = ? AND period = ? AND path = ? AND time = ?;"))

(defn useq
  "Yields a cassandra use statement for a keyspace"
  [keyspace]
  (format "USE %s;" (name keyspace)))

;;
;; The next section contains a series of path matching functions


(defmulti aggregate-with
  "This transforms a raw list of points according to the provided aggregation
   method. Each point is stored as a list of data points, so multiple
   methods make sense (max, min, mean). Additionally, a raw method is
   provided"
  (comp first list))

(defmethod aggregate-with :mean
  [_ {:keys [data] :as metric}]
  (if (seq data)
    (-> metric
        (dissoc :data)
        (assoc :metric (/ (reduce + 0.0 data) (count data))))
    metric))

(defmethod aggregate-with :sum
  [_ {:keys [data] :as metric}]
  (-> metric
      (dissoc :data)
      (assoc :metric (reduce + 0.0 data))))

(defmethod aggregate-with :max
  [_ {:keys [data] :as metric}]
  (-> metric
      (dissoc :data)
      (assoc :metric (apply max data))))

(defmethod aggregate-with :min
  [_ {:keys [data] :as metric}]
  (-> metric
      (dissoc :data)
      (assoc :metric (apply min data))))

(defmethod aggregate-with :raw
  [_ {:keys [data] :as metric}]
  (-> metric
      (dissoc :data)
      (assoc :metric data)))

(defn max-points "Returns the maximum number of points to expect for a given resolution, time range and number of paths"
  [paths rollup from to]
  (-> (- to from)
      (/ rollup)
      (long)
      (inc)
      (* (count paths))
      (int)))

(defn fill-in
  "Fill in fetched data with nil metrics for a given time range"
  [nils [path data]]
  (hash-map path
            (->> (group-by :time data)
                 (merge nils)
                 (map (comp first val))
                 (sort-by :time)
                 (map :metric))))

(defn- batch
  "Creates a batch of prepared statements"
  [^PreparedStatement s values]
  (let [b (BatchStatement.)]
    (doseq [v values]
      (if (some number? (second v)) 
        (.add b (.bind s (into-array Object v)))))
    b))

; Function for creating a prepared statements object
; since we can only prepare a SQL statement once
(defn getprepstatements
  [sql]
  (@p sql))

; Add prepared statements and index by table name
(defn addprepstatements
  [session sql]
  (if-not (getprepstatements sql)
  (swap! p assoc sql (alia/prepare session sql))))

(defn getprocrollups
  [rollstr]
  (@procrollups rollstr))

; Add prepared statements and index by table name
(defn addprocrollups
  [rollstr time]
  (swap! procrollups assoc rollstr time))

; Group these data by table name and consolidate the values.  We will
; ignore other components of these data being passed in.
(defn groupvalues 
  [data]
  (->> (group-by :rollup data)
       (map (fn [[k v ]]
              {:table (get (mapv second (mapv first v)) 0) :v (mapv second (mapv second v)) :rollup k}))
       (into [])))

; Clojure doesn't have a for loop like C, so we have to make it do that
(defn enum 
  [s]
   (map vector (range) s))

; Cassandra returns lists of maps for values, which we needo
; turn into an average
(defn averagerollup
  [data]
  (if (not= 0 (count data))(/ (apply + data) (count data)) nil))
   
(defn cassandra-metric-store
  "Connect to cassandra and start a path fetching thread.
   The interval is fixed for now, at 1minute"
  [{:keys [keyspace cluster hints repfactor chan_size batch_size username password ]
           chan_size 10000
           batch_size 500}]
  (info "creating cassandra metric store")
(let [cluster (if (sequential? cluster) cluster [cluster]) 
      rrpolicy (lb/round-robin-policy)
      session (-> {:load-balancing-policy (lb/token-aware-policy rrpolicy)
                   :contact-points cluster
                   :jmx-reporting? true
                   :keep-alive? true
                   :compression :lz4
                   :max-connections-per-host  {:remote 50  :local 200}}
          (cond-> (and username password)
                   (assoc :credentials {:user username :password password}))
          (alia/cluster)
          (alia/connect keyspace)) ]
    (reify
      Metricstore
      (channel-for [this]
        (let [ch   (chan chan_size)
              ch-p (partition-or-time batch_size ch batch_size 5)]
          (go-forever
           (let [payload (<! ch-p)]
             (try
               (let [inserts (map
                            #(let [{:keys [table metric path time rollup period ttl]} %]
                                { :table table :v [(int ttl) [metric] (int rollup) (int period) path time] :rollup rollup })
                            payload) 
                     paths (map
                            #(let [{:keys [path time]} %] { :path path :time time}) payload) 
                    ]
                    (let [ [lowtable [lowttl lowmetric lowroll lowperiod lowpath lowtime] lowrollup] (vals (apply min-key :rollup inserts)) ]
                        (doseq [[idx i] (enum (sort-by :rollup (groupvalues inserts)))]
                            (if (= idx 0)
                                (let [ [table v rollup] (vals i)
                                    sql (makeinsertquery table)]
                                    (addprepstatements session sql)
                                    (take!
                                        (alia/execute-chan session (batch (getprepstatements sql) v) {:consistency :any})
                                        (fn [rows-or-e]
                                            (if (instance? Throwable rows-or-e)
                                                (info rows-or-e "Cassandra error")
                                            )))))
				            (if (and ( > idx 0) (seq paths))
                                (let [ [table vs rollup] (vals i)
                                        fetchsql (makerollupfetchquery lowtable)
                                        insertsql (makerollupinsertquery table)
                                        [ttl metric vrollup period junk time] (first vs)
                                        fstmt (getprepstatements fetchsql)
                                        istmt (getprepstatements insertsql)
                                        rollpaths (keys (group-by :path (sort-by :path paths))) ]
                                    (addprepstatements session fetchsql)
                                    (addprepstatements session insertsql)
                                    ; Do rollups along boundaries only and try to prevent re-rolls
                                    (doseq [ rollpath rollpaths ] 
                                        (let [ rollstr (str rollpath rollup)
                                                rollvar (or (getprocrollups rollstr) time)]
                                            (if (<= rollvar time) ( 
                                                (addprocrollups rollstr (+ rollup time))
                                                ; process all the rollups
                                                (try
                                                    (take! 
                                                        (alia/execute-chan session fstmt 
                                                            {:values [rollpath lowrollup lowperiod (- time rollup) time]
                                                             :fetch-size Integer/MAX_VALUE
                                                             :consistency :local-one})
                                                        (fn [rows-or-e]
                                                            (if (instance? Throwable rows-or-e)
                                                                (info rows-or-e "Cassandra error")
                                                                (if (seq rows-or-e) 
                                                                    (let [ data (first (vals (apply merge-with concat rows-or-e)))
                                                                        avg (averagerollup data) ]
                                                                        (if (number? avg)
                                                                            (alia/execute-chan session istmt 
                                                                                {:values [(int ttl) [avg] (int rollup) (int period) rollpath time]
                                                                                :consistency :any})))))))
                                                    (catch Exception e
                                                        (info e (str "Rollup processing exception on path: " rollpath (.getMessage e))))))))))))))
             (catch Exception e
                    (info e (str "Store processing exception: " (.getMessage e)))))))
	ch))
	(fetch [this agg table paths tenant rollup period from to]
		(addprepstatements session (makefetchquery table))
		(debug "fetching paths from store: " table paths rollup period from to)
		(if-let [data (and (seq paths)
                           (->> (alia/execute
                                 session (getprepstatements (makefetchquery table))
                                 {:values [paths (int rollup) (int period)
                                           from to]
                                  :fetch-size Integer/MAX_VALUE
                                  :consistency :local-one})
                                (map (partial aggregate-with (keyword agg)))
                                (seq)))]
          (let [ min-point  (-> from (quot rollup) (* rollup))
                 max-point  (-> to (quot rollup) (* rollup))
                 nil-points (->> (range min-point (inc max-point) rollup)
                                (map (fn [time] {time [{:time time}]}))
                                (reduce merge {}))
                 by-path    (->> (group-by :path data)
                                (map (partial fill-in nil-points))
                                (reduce merge {}))]
                {:from min-point
                 :to max-point
                 :step rollup
                 :series by-path})
          {:from from
           :to to
           :step rollup
           :series {}})))))
