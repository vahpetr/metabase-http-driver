(ns metabase.driver.http
  "HTTP API driver."
  (:require [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [metabase.driver :as driver]
            [metabase.driver.http.query-processor :as http.qp]
            [metabase.query-processor.store :as qp.store]
            [metabase.util :as u]
            [metabase.util
             [i18n :refer [trs]]]))

(defn find-first
  [f coll]
  (first (filter f coll)))

(defn- database->definitions
  [database]
  (json/parse-string (:definitions (:details database)) keyword))

(defn- database->table-defs
  [database]
  (or (:tables (database->definitions database)) []))

(defn- database->table-def
  [database name]
  (first (filter #(= (:name %) name) (database->table-defs database))))

(defn table-def->field
  [table-def name]
  (find-first #(= (:name %) name) (:fields table-def)))

(defn mbql-field->expression
  [table-def expr]
  (let [field (table-def->field table-def (:field-name expr))]
    (or (:expression field) (:name field))))

(defn mbql-aggregation->aggregation
  [table-def mbql-aggregation]
  (if (:field mbql-aggregation)
    [(:aggregation-type mbql-aggregation)
     (mbql-field->expression table-def (:field mbql-aggregation))]
    [(:aggregation-type mbql-aggregation)]))

(def json-type->base-type
  {:string  :type/Text
   :number  :type/Float
   :boolean :type/Boolean})

(driver/register! :http)

(defmethod driver/supports? [:http :basic-aggregations] [_ _] false)

(defmethod driver/supports? [:http :native-parameters] [_ _]  true)

(defmethod driver/can-connect? :http [_ _]
  true)

(defmethod driver/describe-database :http [_ database]
  (let [table-defs (database->table-defs database)]
    {:tables (set (for [table-def table-defs]
                    {:name   (:name table-def)
                     :schema (:schema table-def)}))}))

(defmethod driver/describe-table :http [_ database table]
  (let [table-def  (database->table-def database (:name table))]
    {:name   (:name table-def)
     :schema (:schema table-def)
     :fields (set (for [field (:fields table-def)]
                    {:name          (:name field)
                     :database-type (:type field)
                     :base-type     (or (:base_type field)
                                        (json-type->base-type (keyword (:type field))))}))}))

(defmethod driver/mbql->native :http [_ query]
  (let [database    (qp.store/database)
        table       (qp.store/table (:source-table (:query query)))
        table-def   (database->table-def database (:name table))
        breakout    (map (partial mbql-field->expression table-def) (:breakout (:query query)))
        aggregation (map (partial mbql-aggregation->aggregation table-def) (:aggregation (:query query)))]
    {:query (merge (select-keys table-def [:method :url :headers])
                   {:result (merge (:result table-def)
                                   {:breakout     breakout
                                    :aggregation  aggregation})})
     :mbql? true}))

(defn create-regex [key]
  (-> (str "\\{\\{" key "\\}\\}")
      (re-pattern)))
        
(defn replace-in-query [query values]
  (let [pattern (create-regex (get values :label))]
          (clojure.string/replace query pattern (get values :value))))

(defn parameters-to-replace [parameters]
  (for [param parameters]
      (let [template (((get param :target) 1) 1)
            value (get param :value)]      
          {:label template :value value})))

(defmethod driver/substitute-native-parameters :http
  [_ {:keys [query] :as inner-query}]
  (let [params (get inner-query :parameters) 
        values (parameters-to-replace params)]
      {:query (reduce replace-in-query query values)}))

(defmethod driver/execute-reducible-query :http [_ {native-query :native}]
  (log/info (trs "Calling Http Api with the following query: {0}" native-query))
  (http.qp/execute-http-request native-query)) 
