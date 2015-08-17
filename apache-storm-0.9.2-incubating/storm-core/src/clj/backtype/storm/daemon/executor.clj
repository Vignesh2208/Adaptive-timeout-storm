;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.


(ns backtype.storm.daemon.executor
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap])
  (:import [backtype.storm.hooks ITaskHook])
  (:import [backtype.storm.utils End_to_End])
  (:import [backtype.storm.tuple Tuple])
  (:import [java.util ArrayList HashMap List Map Random LinkedHashMap])
  (:import [java.lang Integer Object])
  (:import [backtype.storm.spout ISpoutWaitStrategy])
  (:import [backtype.storm.hooks.info SpoutAckInfo SpoutFailInfo
            EmitInfo BoltFailInfo BoltAckInfo BoltExecuteInfo])
  (:import [backtype.storm.metric.api IMetric IMetricsConsumer$TaskInfo IMetricsConsumer$DataPoint StateMetric])
  (:import [backtype.storm Config])
  (import [java.io BufferedWriter FileWriter File])
  
  (:require [backtype.storm [tuple :as tuple]])
  (:require [backtype.storm.daemon [task :as task]])
  (:require [backtype.storm.daemon.builtin-metrics :as builtin-metrics]))

(bootstrap)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;NEW function additions here


;; New-addition
(defn lambda-estimator [worker]
 (let[ tuple-rate-map (:incoming-tuple-rate-map worker)
       tuple-count-map (:incoming-tuple-count-map worker)
      ]
  (fn[dst_task]
              
            
             (let [start-time (.get tuple-rate-map "start-time")
                   curr-time (current-time-millis)
                   time-diff (time-delta-ms start-time)
                   lambda
                   (do
                     (if (.containsKey tuple-rate-map (str dst_task))
                       
                         (.get tuple-rate-map (str dst_task))
                         0
                       )
                     
                   )                   
                   ]
                   
                                   
             lambda)
                                 
             )
    )
)


(def log-file (str Constants/TIMEOUT_FILE_BASE_DIR  "debug/log_generic.txt"))
(def log-tuple-action (str Constants/TIMEOUT_FILE_BASE_DIR "debug/log_tuple_action.txt"))
(def log-spout-msg (str Constants/TIMEOUT_FILE_BASE_DIR "debug/log_spout.txt"))

(def lock-obj (Object.))
(def is-debug-enabled (MutableObject. (int 0)))
(def is-debug-log-enabled (MutableObject. (int 0)))


(defn set-debug-enabled [^Map conf]
 (if (.containsKey conf Config/DEBUG_PRINT_ENABLED)
  (if (= (.get conf Config/DEBUG_PRINT_ENABLED) true)
   (.setObject is-debug-enabled 1)
   (.setObject is-debug-enabled 0)
  )
  (.setObject is-debug-enabled 0)
 )
)

(defn set-debug-log-enabled [^Map conf]
 (if (.containsKey conf Config/DEBUG_LOG_ENABLED)
   (if (= (.get conf Config/DEBUG_LOG_ENABLED) true)
     (.setObject is-debug-log-enabled 1)
     (.setObject is-debug-log-enabled 0)
   )
   (.setObject is-debug-log-enabled 0)
 )
)


(defn debug-print [& args]
 (if (= (.getObject is-debug-enabled) 1)
   (println (str args))   
 )
)

(defn is-adaptive-timeout-enabled [^Map conf]
  (if (.containsKey conf Config/ADAPTIVE_TIMEOUT_ENABLED) 
    (if (.get conf Config/ADAPTIVE_TIMEOUT_ENABLED)
      true
      false
    )
    false
  )
)  

(defn return-adaptive-timeout-mode [^Map conf]
  (if (is-adaptive-timeout-enabled conf)
   (.get conf Config/ADAPTIVE_TIMEOUT_MODE)
   (str "NORMAL")
  )
  
)

(defn return-base-dir-name [^Map conf]
  (if (.containsKey conf Config/BASE_DIR_NAME)
    (.get conf Config/BASE_DIR_NAME)
    (str "Default_Base_Directory")
  )
  
)

(defn return-topology-specific-info [^Map conf]
  (if (.containsKey conf Config/TOPOLOGY_SPECIFIC_INFO)
    (.get conf Config/TOPOLOGY_SPECIFIC_INFO)
    (str "Default_Topology_Info")
  )
  
)


(defn mk-timeout-timer [timer-name]
  (mk-timer :kill-fn (fn [t]
                       (log-error t "Error when processing event")
                       (halt-process! 20 "Error when processing an event")
                       )
            :timer-name timer-name))

(defn mk-timeout-timer-map [task-ids]
  (let [timeout-timer-map (java.util.HashMap.)]
    (fast-list-iter [task-id task-ids]
         (.put timeout-timer-map task-id (mk-timeout-timer (str "timeout-timer " (str task-id))))
    )  
    
    
  timeout-timer-map)
  
 )



;;New-addition
(defn timeout-read-filename [component-id] 
  (let [filename (str Constants/TIMEOUT_FILE_BASE_DIR component-id ".txt")
        
        ]
  (with-open [wrt (clojure.java.io/writer filename)]
    (.write wrt "30"))
  filename)
  
 )
(defn init-log-files []
  (if (= (.getObject is-debug-log-enabled) 1)
   (do
   
   (.mkdirs (File. (str Constants/TIMEOUT_FILE_BASE_DIR "debug")))
   (with-open [wrt (clojure.java.io/writer log-file)]
   (.write wrt "##### New Log-file #####\n"))
   (with-open [wrt (clojure.java.io/writer log-spout-msg)]
   (.write wrt "##### New log-spout-msg-log #####\n"))
   (with-open [wrt (clojure.java.io/writer log-tuple-action)]
   (.write wrt "##### New log-tuple-action-log #####\n"))
   )
  )
)

(defn write-message [filename message component-id]
  (if (= (.getObject is-debug-log-enabled) 1)
    (spit filename (str "Executor ID : " component-id ", msg : " message) :append true)
  )
)
;; Function which reads the timeout value from the giben file if it exist. Otherwise it outputs a default value of 30 sec
(defn read_timeout []
  
  (fn [filename]
    (locking lock-obj
    (let [value_read 
         (if  (.exists (clojure.java.io/file filename))
    	     (do 
               
               (let [temp (slurp filename)             
                      
                      
                 val (if (not (= temp nil))
                       (try
                                                 
                         (read-string temp)
                         (catch NumberFormatException e
                           (debug-print "NumberFormatException")
                         30)
                        )
                       30                        
                     )
                 ]
    	       
            (debug-print "Read Val = " val)
             val
              
    	      )
              
    
	     )
	 30)
	]
      
    value_read)
    )
    )
    
 )


(defn reset-timeout[filename time-out-value]
 
 (with-open [w (clojure.java.io/writer filename)]
  (.write w (str time-out-value)))
)
    


;;New-addition
(defn timeout-file-map [component-id task-ids] 
  
  (let [file-map (java.util.HashMap.)
        ]
    (fast-list-iter [task-id task-ids]
          (let [filename (str Constants/TIMEOUT_FILE_BASE_DIR component-id "-" task-id ".txt")]
            (.put file-map task-id filename)
            (with-open [wrt (clojure.java.io/writer filename)]
              (.write wrt "30"))
         )
                    
     )
  
  file-map)
 )



(defn setup-timeout-ticks! [worker file-name executor-data dst-task-id]

(let [storm-conf (:storm-conf executor-data)
        	tick-time-secs (storm-conf TOPOLOGY-TICK-TUPLE-FREQ-SECS)
	        receive-queue (:receive-queue executor-data)
        	context (:worker-context executor-data)
        	timeout-fn (:timeout-read-fn executor-data)
          timeout-timer-map (:timeout-timer-map executor-data)         
         ]
        	(debug-print (str "In setup tick time secs for executor : " (:component-id executor-data) "Tic time secs =" tick-time-secs))
        	(when tick-time-secs
        	
			(if (or (system-id? (:component-id executor-data))
	                (and (not (storm-conf TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS))
	                (= :spout (:type executor-data))))
        		  (do
        			   (if (and (= (:component-id executor-data) Constants/ACKER_COMP_ID) (storm-conf TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS)) ;; Enable timeout timers only for ackers
        			     (do
        				       (debug-print (str "Executor ID : " (:component-id executor-data) " Timeout timer enabled for executor " ))
        				       (schedule-refresh-recurring
                        (.get timeout-timer-map dst-task-id)
        				        
                        timeout-fn
        				        file-name        				            				         
        				        (fn []
        					          (debug-print (str "Executor ID : " (:component-id executor-data) " Timeout expired tuple sent from executor" " to : " (str dst-task-id) "at " (current-time-secs)))
                            (write-message log-file (str  "Timeout expired tuple sent from executor\n ") (:component-id executor-data))
        					          (disruptor/publish
        					            receive-queue
        					            [[dst-task-id (TupleImpl. context 
                                                   ["timeout-expired"] 
                                                   Constants/SYSTEM_TASK_ID Constants/SYSTEM_TIMEOUT_STREAM_ID)]]
        				             )
                             (write-message log-file (str "Finished sending timeout expired tuple \n") (:component-id executor-data))
                       ))
        			     )
        			     (log-message "Timeouts disabled for executor " (:component-id executor-data) ":" (:executor-id executor-data))
        			   )
           
        	  
        		  )
        		  
        		  (if (and (storm-conf TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS) 
        		            (= :spout (:type executor-data))) ;; Only enable timeout timer for ackers and spouts 
        			    (do 
        				    (debug-print (str "Executor ID : " (:component-id executor-data) " Timeout timer enabled for executor " ))
        				    (schedule-refresh-recurring        				    
        				     (.get timeout-timer-map dst-task-id)
                     
        				     timeout-fn
        				     file-name
                     (fn []
					             (debug-print (str "Executor ID : " (:component-id executor-data) " Timeout expired tuple sent from executor" " to : " (str dst-task-id) "at " (current-time-secs)))
					             (write-message log-file (str "Timeout expired tuple sent from executor\n") (:component-id executor-data))     
                       
                       (disruptor/publish
					                    receive-queue
					                    [[dst-task-id (TupleImpl. context 
                                                   ["timeout-expired"] 
                                                   Constants/SYSTEM_TASK_ID Constants/SYSTEM_TIMEOUT_STREAM_ID)]]
			                    )
                       (write-message log-file (str " Finished sending timeout expired tuple \n") (:component-id executor-data))
                       ))
		            )
		          )
	          
          )
          )
         )        
	
)

(defn is-fault-injector-enabled [^Map conf]
  (if (.containsKey conf Config/FAULT_INJECTOR_ENABLED) 
    (if (.get conf Config/FAULT_INJECTOR_ENABLED)
      true
      false
    )
    false
  )
)  


(defn return-msg-drop-probability [^Map conf]
  (if (is-fault-injector-enabled conf)
   (.get conf Config/MSG_DROP_PROBABILITY)
   (int 0)
  )
  
)

(defn should-it-be-dropped [msg-drop-probability ^Random rand]
  (if (= msg-drop-probability (int 0))
    (do
      
      false
    )
    (do
          (let [rand-number-max (int msg-drop-probability)
                gen-rand-number (.nextInt rand rand-number-max)
                
              ] 
               
               (if (not (= gen-rand-number (int (- rand-number-max 1))))
                  (do
                      
                      false
                   )
                  (do
                      
                      true
                  )
               )
          )
   )
  
  )
)




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- mk-fields-grouper [^Fields out-fields ^Fields group-fields ^List target-tasks]
  (let [num-tasks (count target-tasks)
        task-getter (fn [i] (.get target-tasks i))]
    (fn [task-id ^List values]
      (-> (.select out-fields group-fields values)
          tuple/list-hash-code
          (mod num-tasks)
          task-getter))))

(defn- mk-shuffle-grouper [^List target-tasks]
  (let [choices (rotating-random-range target-tasks)]
    (fn [task-id tuple]
      (acquire-random-range-id choices))))

(defn- mk-custom-grouper [^CustomStreamGrouping grouping ^WorkerTopologyContext context ^String component-id ^String stream-id target-tasks]
  (.prepare grouping context (GlobalStreamId. component-id stream-id) target-tasks)
  (fn [task-id ^List values]
    (.chooseTasks grouping task-id values)
    ))

(defn- mk-grouper
  "Returns a function that returns a vector of which task indices to send tuple to, or just a single task index."
  [^WorkerTopologyContext context component-id stream-id ^Fields out-fields thrift-grouping ^List target-tasks]
  (let [num-tasks (count target-tasks)
        random (Random.)
        target-tasks (vec (sort target-tasks))]
    (condp = (thrift/grouping-type thrift-grouping)
      :fields
        (if (thrift/global-grouping? thrift-grouping)
          (fn [task-id tuple]
            ;; It's possible for target to have multiple tasks if it reads multiple sources
            (first target-tasks))
          (let [group-fields (Fields. (thrift/field-grouping thrift-grouping))]
            (mk-fields-grouper out-fields group-fields target-tasks)
            ))
      :all
        (fn [task-id tuple] target-tasks)
      :shuffle
        (mk-shuffle-grouper target-tasks)
      :local-or-shuffle
        (let [same-tasks (set/intersection
                           (set target-tasks)
                           (set (.getThisWorkerTasks context)))]
          (if-not (empty? same-tasks)
            (mk-shuffle-grouper (vec same-tasks))
            (mk-shuffle-grouper target-tasks)))
      :none
        (fn [task-id tuple]
          (let [i (mod (.nextInt random) num-tasks)]
            (.get target-tasks i)
            ))
      :custom-object
        (let [grouping (thrift/instantiate-java-object (.get_custom_object thrift-grouping))]
          (mk-custom-grouper grouping context component-id stream-id target-tasks))
      :custom-serialized
        (let [grouping (Utils/deserialize (.get_custom_serialized thrift-grouping))]
          (mk-custom-grouper grouping context component-id stream-id target-tasks))
      :direct
        :direct
      )))

(defn- outbound-groupings [^WorkerTopologyContext worker-context this-component-id stream-id out-fields component->grouping]
  (->> component->grouping
       (filter-key #(-> worker-context
                        (.getComponentTasks %)
                        count
                        pos?))
       (map (fn [[component tgrouping]]
               [component
                (mk-grouper worker-context
                            this-component-id
                            stream-id
                            out-fields
                            tgrouping
                            (.getComponentTasks worker-context component)
                            )]))
       (into {})
       (HashMap.)))

(defn outbound-components
  "Returns map of stream id to component id to grouper"
  [^WorkerTopologyContext worker-context component-id]
  (->> (.getTargets worker-context component-id)
        clojurify-structure
        (map (fn [[stream-id component->grouping]]
               [stream-id
                (outbound-groupings
                  worker-context
                  component-id
                  stream-id
                  (.getComponentOutputFields worker-context component-id stream-id)
                  component->grouping)]))
         (into {})
         (HashMap.)))

(defn executor-type [^WorkerTopologyContext context component-id]
  (let [topology (.getRawTopology context)
        spouts (.get_spouts topology)
        bolts (.get_bolts topology)]
    (cond (contains? spouts component-id) :spout
          (contains? bolts component-id) :bolt
          :else (throw-runtime "Could not find " component-id " in topology " topology))))

(defn executor-selector [executor-data & _] (:type executor-data))

(defmulti mk-threads executor-selector)
(defmulti mk-executor-stats executor-selector)
(defmulti close-component executor-selector)

(defn- normalized-component-conf [storm-conf general-context component-id]
  (let [to-remove (disj (set ALL-CONFIGS)
                        TOPOLOGY-DEBUG
                        TOPOLOGY-MAX-SPOUT-PENDING
                        TOPOLOGY-MAX-TASK-PARALLELISM
                        TOPOLOGY-TRANSACTIONAL-ID
                        TOPOLOGY-TICK-TUPLE-FREQ-SECS
                        TOPOLOGY-SLEEP-SPOUT-WAIT-STRATEGY-TIME-MS
                        TOPOLOGY-SPOUT-WAIT-STRATEGY
                        )
        spec-conf (-> general-context
                      (.getComponentCommon component-id)
                      .get_json_conf
                      from-json)]
    (merge storm-conf (apply dissoc spec-conf to-remove))
    ))

(defprotocol RunningExecutor
  (render-stats [this])
  (get-executor-id [this]))

(defn throttled-report-error-fn [executor]
  (let [storm-conf (:storm-conf executor)
        error-interval-secs (storm-conf TOPOLOGY-ERROR-THROTTLE-INTERVAL-SECS)
        max-per-interval (storm-conf TOPOLOGY-MAX-ERROR-REPORT-PER-INTERVAL)
        interval-start-time (atom (current-time-secs))
        interval-errors (atom 0)
        ]
    (fn [error]
      (log-error error)
      (write-message log-spout-msg (str " Throttled Error\n") (:component-id executor))
      (when (> (time-delta @interval-start-time)
               error-interval-secs)
        (reset! interval-errors 0)
        (reset! interval-start-time (current-time-secs)))
      (swap! interval-errors inc)

      (when (<= @interval-errors max-per-interval)
        (cluster/report-error (:storm-cluster-state executor) (:storm-id executor) (:component-id executor) error)
        ))))

;; in its own function so that it can be mocked out by tracked topologies
(defn mk-executor-transfer-fn [batch-transfer->worker]
  (fn this
    ([task tuple block? ^List overflow-buffer]
      (if (and overflow-buffer (not (.isEmpty overflow-buffer)))
        (.add overflow-buffer [task tuple])
        (try-cause
          (disruptor/publish batch-transfer->worker [task tuple] block?)
        (catch InsufficientCapacityException e
          (if overflow-buffer
            (.add overflow-buffer [task tuple])
            (throw e))
          ))))
    ([task tuple overflow-buffer]
      (this task tuple (nil? overflow-buffer) overflow-buffer))
    ([task tuple]
      (this task tuple nil)
      )))

(defn mk-executor-data [worker executor-id]
  (let [worker-context (worker-context worker)
        task-ids (executor-id->tasks executor-id)
        component-id (.getComponentId worker-context (first task-ids))
        storm-conf (normalized-component-conf (:storm-conf worker) worker-context component-id)
        executor-type (executor-type worker-context component-id)
        batch-transfer->worker (disruptor/disruptor-queue
                                  (str "executor"  executor-id "-send-queue")
                                  (storm-conf TOPOLOGY-EXECUTOR-SEND-BUFFER-SIZE)
                                  :claim-strategy :single-threaded
                                  :wait-strategy (storm-conf TOPOLOGY-DISRUPTOR-WAIT-STRATEGY))
	      ;;timeout-file-name (timeout-read-filename component-id)
        timeout-map (timeout-file-map component-id task-ids)
        ^HashMap inter-arrival-time-map (:task-inter-arrival-time-map worker)
        ^HashMap queuing-delay-start-map (:task-queuing-delay-start-map worker)
        ]
    (recursive-map
     :worker worker
     :worker-context worker-context
     :executor-id executor-id
     :task-ids task-ids
     :component-id component-id
     
     
     ;;timeout-related
     :timeout-read-fn (read_timeout)
     ;;:timeout-file timeout-file-name
     :timeout-map timeout-map
     :timeout-timer-map (mk-timeout-timer-map task-ids)
     :lambda-estimator-fn (lambda-estimator worker)
     :comp-start-time (System/currentTimeMillis)
     :inter-arrival-time-map inter-arrival-time-map
     :queuing-delay-start-map queuing-delay-start-map
     
     
     
     :open-or-prepare-was-called? (atom false)
     :storm-conf storm-conf
     :receive-queue ((:executor-receive-queue-map worker) executor-id)
     :storm-id (:storm-id worker)
     :conf (:conf worker)
     :shared-executor-data (HashMap.)
     :storm-active-atom (:storm-active-atom worker)
     :batch-transfer-queue batch-transfer->worker
     :transfer-fn (mk-executor-transfer-fn batch-transfer->worker)
     :suicide-fn (:suicide-fn worker)
     :storm-cluster-state (cluster/mk-storm-cluster-state (:cluster-state worker))
     :type executor-type
     ;; TODO: should refactor this to be part of the executor specific map (spout or bolt with :common field)
     :stats (mk-executor-stats <> (sampling-rate storm-conf))
     :interval->task->metric-registry (HashMap.)
     :task->component (:task->component worker)
     :stream->component->grouper (outbound-components worker-context component-id)
     :report-error (throttled-report-error-fn <>)
     :report-error-and-die (fn [error]
                             ((:report-error <>) error)
                             ((:suicide-fn <>)))
     :deserializer (KryoTupleDeserializer. storm-conf worker-context)
     :sampler (mk-stats-sampler storm-conf)
     ;; TODO: add in the executor-specific stuff in a :specific... or make a spout-data, bolt-data function?
     )))

(defn start-batch-transfer->worker-handler! [worker executor-data]
  (let [worker-transfer-fn (:transfer-fn worker)
        cached-emit (MutableObject. (ArrayList.))
        storm-conf (:storm-conf executor-data)
        serializer (KryoTupleSerializer. storm-conf (:worker-context executor-data))
        ]
    (disruptor/consume-loop*
      (:batch-transfer-queue executor-data)
      (disruptor/handler [o seq-id batch-end?]
        (let [^ArrayList alist (.getObject cached-emit)]
          (.add alist o)
          (when batch-end?
            (worker-transfer-fn serializer alist)
            (.setObject cached-emit (ArrayList.))
            )))
      :kill-fn (:report-error-and-die executor-data))))

(defn setup-metrics! [executor-data]
  (let [{:keys [storm-conf receive-queue worker-context interval->task->metric-registry]} executor-data
        distinct-time-bucket-intervals (keys interval->task->metric-registry)]
    (doseq [interval distinct-time-bucket-intervals]
      (schedule-recurring 
       (:user-timer (:worker executor-data)) 
       interval
       interval
       (fn []
         (disruptor/publish
          receive-queue
          [[nil (TupleImpl. worker-context [interval] Constants/SYSTEM_TASK_ID Constants/METRICS_TICK_STREAM_ID)]]))))))

(defn metrics-tick [executor-data task-data ^TupleImpl tuple]
  (let [{:keys [interval->task->metric-registry ^WorkerTopologyContext worker-context]} executor-data
        interval (.getInteger tuple 0)
        task-id (:task-id task-data)
        name->imetric (-> interval->task->metric-registry (get interval) (get task-id))
        task-info (IMetricsConsumer$TaskInfo.
                    (. (java.net.InetAddress/getLocalHost) getCanonicalHostName)
                    (.getThisWorkerPort worker-context)
                    (:component-id executor-data)
                    task-id
                    (long (/ (System/currentTimeMillis) 1000))
                    interval)
        data-points (->> name->imetric
                      (map (fn [[name imetric]]
                             (let [value (.getValueAndReset ^IMetric imetric)]
                               (if value
                                 (IMetricsConsumer$DataPoint. name value)))))
                      (filter identity)
                      (into []))]
      (if (seq data-points)
        (task/send-unanchored task-data Constants/METRICS_STREAM_ID [task-info data-points]))))

(defn setup-ticks! [worker executor-data task-ids timeout-map]
  (let [storm-conf (:storm-conf executor-data)
        tick-time-secs (storm-conf TOPOLOGY-TICK-TUPLE-FREQ-SECS)
        receive-queue (:receive-queue executor-data)
        context (:worker-context executor-data)
        
        ]
    
    (fast-list-iter [task-id task-ids]
       (if (= false (is-adaptive-timeout-enabled storm-conf))
         (when tick-time-secs
         (reset-timeout (.get timeout-map task-id) tick-time-secs)
         )
      )                      
    )
    (when tick-time-secs
      (if (or (system-id? (:component-id executor-data))
              (and (not (storm-conf TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS))
                   (= :spout (:type executor-data))))
        (do
          (if (= (:component-id executor-data) Constants/ACKER_COMP_ID) ;; Enable only if it is an acker executor - Don't know why it was disabled in the first place
            (schedule-recurring
              (:user-timer worker)
              tick-time-secs
              tick-time-secs
              (fn []
                (write-message log-file (str "Tick tuple sent to acker at " (current-time-secs) "\n") (:component-id executor-data))
                (disruptor/publish
                  receive-queue
                  [[nil (TupleImpl. context [tick-time-secs] Constants/SYSTEM_TASK_ID Constants/SYSTEM_TICK_STREAM_ID)]]
                  )))
            
            (log-message "Timeouts disabled for executor " (:component-id executor-data) ":" (:executor-id executor-data))
           )
          
        )
        (schedule-recurring
          (:user-timer worker)
          tick-time-secs
          tick-time-secs
          (fn []
            (write-message log-file (str "Tick tuple sent to spout at " (current-time-secs) "\n") (:component-id executor-data))
            (disruptor/publish
              receive-queue
              [[nil (TupleImpl. context [tick-time-secs] Constants/SYSTEM_TASK_ID Constants/SYSTEM_TICK_STREAM_ID)]]
              )
            
            ))))))

(defn mk-executor [worker executor-id]
  (let [executor-data (mk-executor-data worker executor-id)
        _ (log-message "Loading executor " (:component-id executor-data) ":" (pr-str executor-id))
        task-datas (->> executor-data
                        :task-ids
                        (map (fn [t] [t (task/mk-task executor-data t)]))
                        (into {})
                        (HashMap.))
        _ (log-message "Loaded executor tasks " (:component-id executor-data) ":" (pr-str executor-id))
        report-error-and-die (:report-error-and-die executor-data)
        component-id (:component-id executor-data)
        
        ;;timeout-related
        ;;filename (:timeout-file executor-data)
        task-ids (:task-ids executor-data)
        timeout-map (:timeout-map executor-data)
        
        ;;;;        
        ;; starting the batch-transfer->worker ensures that anything publishing to that queue 
        ;; doesn't block (because it's a single threaded queue and the caching/consumer started
        ;; trick isn't thread-safe)
        system-threads [(start-batch-transfer->worker-handler! worker executor-data)]
        handlers (with-error-reaction report-error-and-die
                   (mk-threads executor-data task-datas))
        threads (concat handlers system-threads)]    
    (setup-ticks! worker executor-data task-ids timeout-map)
    (set-debug-enabled (:storm-conf executor-data))
    (set-debug-log-enabled (:storm-conf executor-data))
    (init-log-files)
    ;; single timeout-file
    ;;(setup-timeout-ticks! worker filename executor-data)
    ;;one timeout file per task
    (fast-list-iter [task-id task-ids]
       (debug-print (str "Setup timeout ticks for task " (str task-id)))
       (setup-timeout-ticks! worker (.get timeout-map task-id) executor-data task-id)
                             
    )

    
    
    (log-message "Finished loading executor " component-id ":" (pr-str executor-id))
    ;; TODO: add method here to get rendered stats... have worker call that when heartbeating
    (reify
      RunningExecutor
      (render-stats [this]
        (stats/render-stats! (:stats executor-data)))
      (get-executor-id [this]
        executor-id )
      Shutdownable
      (shutdown
        [this]
        (log-message "Shutting down executor " component-id ":" (pr-str executor-id))
        (write-message log-file (str " Shutting down executor \n" ) component-id) 
        (disruptor/halt-with-interrupt! (:receive-queue executor-data))
        (disruptor/halt-with-interrupt! (:batch-transfer-queue executor-data))
        
        ;;timeout-related
        (fast-list-iter [task-id task-ids]
         (cancel-timer (.get (:timeout-timer-map executor-data) task-id))               
        )    
        (try
                   
          (fast-list-iter [task-id task-ids]
           (debug-print (str "Executor ID : " component-id " Deleting timeout file for " (str task-id) (.get timeout-map task-id)))
           (write-message log-file (str "Deleting timeout file\n" ) component-id)
           (clojure.java.io/delete-file (.get timeout-map task-id))
          )
         (catch Exception e (str "Caught exception during removal of time-out-file: " (.getMessage e))))
        ;;;;
        
        
        
        
        (doseq [t threads]
          (.interrupt t)
          (.join t))
        
        (doseq [user-context (map :user-context (vals task-datas))]
          (doseq [hook (.getHooks user-context)]
            (.cleanup hook)))
        (.disconnect (:storm-cluster-state executor-data))
        (when @(:open-or-prepare-was-called? executor-data)
          (doseq [obj (map :object (vals task-datas))]
            (close-component executor-data obj)))
        (log-message "Shut down executor " component-id ":" (pr-str executor-id)))
        )))

(defn- fail-spout-msg [executor-data task-data msg-id tuple-info time-delta]
  (let [^ISpout spout (:object task-data)
        task-id (:task-id task-data)]
    ;;TODO: need to throttle these when there's lots of failures
    (log-debug "Failing message " msg-id ": " tuple-info)
    (.fail spout msg-id)
    (task/apply-hooks (:user-context task-data) .spoutFail (SpoutFailInfo. msg-id task-id time-delta))
    (when time-delta
      (builtin-metrics/spout-failed-tuple! (:builtin-metrics task-data) (:stats executor-data) (:stream tuple-info))      
      (stats/spout-failed-tuple! (:stats executor-data) (:stream tuple-info) time-delta))))

(defn- ack-spout-msg [executor-data task-data msg-id tuple-info time-delta]
  (let [storm-conf (:storm-conf executor-data)
        ^ISpout spout (:object task-data)
        task-id (:task-id task-data)]
    (when (= true (storm-conf TOPOLOGY-DEBUG))
      (log-message "Acking message " msg-id))
    (.ack spout msg-id)
    (task/apply-hooks (:user-context task-data) .spoutAck (SpoutAckInfo. msg-id task-id time-delta))
    (when time-delta
      (builtin-metrics/spout-acked-tuple! (:builtin-metrics task-data) (:stats executor-data) (:stream tuple-info) time-delta)
      (stats/spout-acked-tuple! (:stats executor-data) (:stream tuple-info) time-delta))))

(defn mk-task-receiver [executor-data tuple-action-fn]
  (let [^KryoTupleDeserializer deserializer (:deserializer executor-data)
        task-ids (:task-ids executor-data)
        component-id (:component-id executor-data)
        debug? (= true (-> executor-data :storm-conf (get TOPOLOGY-DEBUG)))
        ]
    (disruptor/clojure-handler
      (fn [tuple-batch sequence-id end-of-batch?]
        (fast-list-iter [[task-id msg] tuple-batch]
          (let [^TupleImpl tuple (if (instance? Tuple msg) msg (.deserialize deserializer msg))]
            (when debug? (log-message "Processing received message " tuple))
            (write-message log-tuple-action (str "Processing received message \n") component-id)
            (try
              (if task-id
                (tuple-action-fn task-id tuple)
                ;; null task ids are broadcast tuples
                (fast-list-iter [task-id task-ids]
                  (tuple-action-fn task-id tuple)
                  ))
             (catch Exception e 
               (write-message log-tuple-action (str "Exception caught in message processing : Tuple = " tuple " at " component-id (.getMessage e) "\n") component-id)
               
               ))
            (write-message log-tuple-action (str "Processed received message \n") component-id)
            ))))))

(defn executor-max-spout-pending [storm-conf num-tasks]
  (let [p (storm-conf TOPOLOGY-MAX-SPOUT-PENDING)]
    (if p (* p num-tasks))))

(defn init-spout-wait-strategy [storm-conf]
  (let [ret (-> storm-conf (get TOPOLOGY-SPOUT-WAIT-STRATEGY) new-instance)]
    (.prepare ret storm-conf)
    ret
    ))

(defmethod mk-threads :spout [executor-data task-datas]
  (let [{:keys [storm-conf component-id worker-context transfer-fn report-error sampler open-or-prepare-was-called?]} executor-data
        ^ISpoutWaitStrategy spout-wait-strategy (init-spout-wait-strategy storm-conf)
        max-spout-pending (executor-max-spout-pending storm-conf (count task-datas))
        ^Integer max-spout-pending (if max-spout-pending (int max-spout-pending))        
        last-active (atom false)        
        spouts (ArrayList. (map :object (vals task-datas)))
        rand (Random. (Utils/secureRandomLong))
        ^HashMap inter-arrival-time-map (:inter-arrival-time-map executor-data)
        ^HashMap queuing-delay-start-map (:queuing-delay-start-map executor-data)
        
        
        
        
        ;;timeout-related
        timeout-map (:timeout-map executor-data)
        timeout-read-fn (:timeout-read-fn executor-data)
        tick_tuple_received (MutableLong. 0)
        time_out_set_recently (MutableLong. 0)
        lowest_acker_id_seen (MutableLong. 10000)
        acker_task_id_list (java.util.ArrayList.)
        overflow-buffer (LinkedList.)
        adaptive-timeout-mode (return-adaptive-timeout-mode storm-conf)
        adaptive-timeout-base-dir (return-base-dir-name storm-conf)
        adaptive-timeout-topology-specific-info (return-topology-specific-info storm-conf)
        adaptive-timeout-obj (java.util.HashMap.)
        start-timestamp(MutableObject. (long 0))
        stop-timestamp (MutableObject. (long 0))  
        queuing-delay (MutableObject. (long 0))
        inter-arrival-time (MutableObject. (long 0))
        
        ;;;;;;
        
        
        pending (RotatingMap.
                 2 ;; microoptimize for performance of .size method
                 (reify RotatingMap$ExpiredCallback
                   (expire [this msg-id [task-id spout-id tuple-info start-time-ms]]
                     (let [time-delta (if start-time-ms (time-delta-ms start-time-ms))]
                       (fail-spout-msg executor-data (get task-datas task-id) spout-id tuple-info time-delta)
                       ;;;
                      (write-message log-tuple-action "ACK-Fail received at spout \n" component-id)
                      (try
                      (.on_Fail (.get adaptive-timeout-obj (str task-id)) (str spout-id))
                      (catch Exception e (str "Caught exception " (.getMessage e))))
                      (write-message log-tuple-action "ACK-Fail processed at spout \n" component-id)
                      ;;;
                       ))))
        tuple-action-fn (fn [task-id ^TupleImpl tuple]
                          (let [stream-id (.getSourceStreamId tuple)
                                
                                ^LinkedList base-list  (if (.containsKey queuing-delay-start-map (str task-id))
                                                         (.get queuing-delay-start-map (str task-id))
                                                         (.add (java.util.LinkedList.) (System/nanoTime))
                                                        )
                                ;;q-delay-start (if (> (.size base-list) 0)                                                
                                ;;                (.poll base-list)
                                ;;                (System/nanoTime))
                                ]
                            ;;(if (> (.size base-list) 0)
                            ;;  (.removeFirst base-list)
                            ;;)
                            ;;(if (not (= q-delay-start nil))
                            ;;  (.setObject queuing-delay (long (- (System/nanoTime) q-delay-start)))
                            ;;  (.setObject queuing-delay (long 0))
                            ;;)
                            (.setObject inter-arrival-time (long (.get (.get inter-arrival-time-map (str task-id)) 0)))
                            
                            
                            
                            (write-message log-tuple-action (str "Taking action on tuple received at spout over stream-id : " stream-id "\n" ) component-id)
                            
                            (when stream-id
                            (condp = stream-id
                              
                              Constants/SYSTEM_TICK_STREAM_ID    (do                                           
                                                                  
                                                                   ;; For simple approach- call ack with a special message id here to trigger 
                                                                  ;; timeout computation
                                                                  
                                                                   (write-message log-tuple-action "Received tuple is a Tick tuple. Processing\n" component-id)
                                                                   ;;;
                                                                   (try
                                                                    (.on_Tick_tuple (.get adaptive-timeout-obj (str task-id)) (str "timeout compute"))
                                                                   (catch Exception e (str "Caught exception " (.getMessage e))))
                                                                    ;;;
                                                                   (write-message log-tuple-action "Tick tuple Processed at spout\n" component-id)
                                                                  
                                                                   )
                              ACKER-TIMEOUT-DISPATCH-STREAM-ID (do
                                                                (let[timeout-value (int (.getValue tuple 0))
                                                                    time-out-file (str Constants/TIMEOUT_FILE_BASE_DIR component-id "-" task-id ".txt")]
                                                                  (if (<= (int (.getSourceTask tuple)) (.get lowest_acker_id_seen))
                                                                   (do
               
                                                                   (locking lock-obj
                                                                    (with-open [w (clojure.java.io/writer time-out-file)]
                                                                      (.write w (str timeout-value)))
                                                                   )
                                                                    (.set lowest_acker_id_seen (int (.getSourceTask tuple)))
                                                                   )
                                                                  
                                                                  )
                                                                )
                                                                (write-message log-tuple-action (str "Received tuple contains timeout-reset value from Acker\n") component-id)
                                                                )
                              Constants/SYSTEM_TIMEOUT_STREAM_ID (do 
                                                                  
                                                                   ;; call pending.rotate here
                                                                   
                                                                   (write-message log-tuple-action "Timeout expired tuple received at spout\n" component-id)
                                                                   (.rotate pending)
                                                                   (write-message log-tuple-action "Timeout expired tuple Processed at spout\n" component-id)
                                                                  ) 
                                                                  
                                                                  
                              Constants/METRICS_TICK_STREAM_ID   (do
                                                                   (write-message log-tuple-action "Metrics tuple received at spout\n" component-id)
                                                                   (metrics-tick executor-data (get task-datas task-id) tuple)
                                                                   (write-message log-tuple-action "Metrics tuple processed at spout\n" component-id)
                                                                   )
                              (do
                                (write-message log-tuple-action (str "Received a regular tuple at spout\n") component-id)
                              (let [id (.getValue tuple 0)
                                    [stored-task-id spout-id tuple-finished-info start-time-ms] (.remove pending id)]
                                (when spout-id
                                  (when-not (= stored-task-id task-id)
                                    (write-message log-tuple-action (str "Run time exception at spout\n") component-id)
                                    (throw-runtime "Fatal error, mismatched task ids: " task-id " " stored-task-id))
                                  (let [time-delta (if start-time-ms (time-delta-ms start-time-ms))]
                                    (condp = stream-id
                                      
                                      ACKER-ACK-STREAM-ID    (do
                                                             (write-message log-tuple-action "Received ACK tuple at spout\n" component-id)
                                                             (ack-spout-msg executor-data (get task-datas task-id)
                                                                            spout-id tuple-finished-info time-delta)
                                                             (if (not (.contains acker_task_id_list (.getSourceTask tuple)))
                                                               (.add acker_task_id_list (.getSourceTask tuple))
                                                              )
                                                             
                                                             ;;;
                                                             (try
                                                             (.on_Ack (.get adaptive-timeout-obj (str task-id)) (str spout-id))
                                                             (catch Exception e (str "Caught exception " (.getMessage e))))
                                                             ;;;
                                                             
                                                             (write-message log-tuple-action "Ack processed at spout\n" component-id)
                                                             
                                                            )
                                      ACKER-FAIL-STREAM-ID    (do
                                                                
                                                               (write-message log-tuple-action "Received ACK-fail at spout\n" component-id)
                                                                (fail-spout-msg executor-data (get task-datas task-id)
                                                                                spout-id tuple-finished-info time-delta)
                                                                 (if (not (.contains acker_task_id_list (.getSourceTask tuple)))
                                                                   (.add acker_task_id_list (.getSourceTask tuple))
                                                                   )
                                                                 
                                                             ;;;
                                                             (try
                                                             (.on_Fail (.get adaptive-timeout-obj (str task-id)) (str spout-id))
                                                             (catch Exception e (str "Caught exception " (.getMessage e))))
                                                             ;;;
                                                                 (write-message log-tuple-action "ACK-Fail processed at spout\n" component-id)
                                                               )
                                                            (write-message log-tuple-action (str "Received message not recognized at spout\n") component-id)
                                      )))
                                ;; TODO: on failure, emit tuple to failure stream
                                )
                                ;(write-message log-tuple-action (str "Action taken on tuple\n") component-id)
                              )
                              ))
                            
                            ))
        receive-queue (:receive-queue executor-data)
        event-handler (mk-task-receiver executor-data tuple-action-fn)
        has-ackers? (has-ackers? storm-conf)
        emitted-count (MutableLong. 0)
        empty-emit-streak (MutableLong. 0)
        
        ;; the overflow buffer is used to ensure that spouts never block when emitting
        ;; this ensures that the spout can always clear the incoming buffer (acks and fails), which
        ;; prevents deadlock from occuring across the topology (e.g. Spout -> Bolt -> Acker -> Spout, and all
        ;; buffers filled up)
        ;; when the overflow buffer is full, spouts stop calling nextTuple until it's able to clear the overflow buffer
        ;; this limits the size of the overflow buffer to however many tuples a spout emits in one call of nextTuple, 
        ;; preventing memory issues
        ]
   
    [(async-loop
      (fn []
        ;; If topology was started in inactive state, don't call (.open spout) until it's activated first.
        (while (not @(:storm-active-atom executor-data))
          (write-message log-tuple-action "Inactive-state\n" component-id)
          (Thread/sleep 100))
        (write-message log-spout-msg "Async-loop resumes\n" component-id)
        (log-message "Opening spout " component-id ":" (keys task-datas))
        (doseq [[task-id task-data] task-datas
                :let [^ISpout spout-obj (:object task-data)
                      tasks-fn (:tasks-fn task-data)
                      
                      ;;;
                      end-to-end (End_to_End. (str Constants/TIMEOUT_FILE_BASE_DIR) (str component-id) (str task-id) (str adaptive-timeout-base-dir) (str adaptive-timeout-topology-specific-info) (str adaptive-timeout-mode) (.getObject is-debug-log-enabled))

                      ;;;
                      
                      
                      send-spout-msg (fn [out-stream-id values message-id out-task-id]
                                       (.increment emitted-count)
                                                                                                                   
                                       (let [out-tasks (if out-task-id
                                                         (tasks-fn out-task-id out-stream-id values)
                                                         (tasks-fn out-stream-id values))
                                             rooted? (and message-id has-ackers?)
                                             root-id (if rooted? (MessageId/generateId rand))
                                             out-ids (fast-list-for [t out-tasks] (if rooted? (MessageId/generateId rand)))]
                                         (fast-list-iter [out-task out-tasks id out-ids]
                                                         (let [tuple-id (if rooted?
                                                                          (MessageId/makeRootId root-id id)
                                                                          (MessageId/makeUnanchored))
                                                               out-tuple (TupleImpl. worker-context
                                                                                     values
                                                                                     task-id
                                                                                     out-stream-id
                                                                                     tuple-id)]
                                                           (transfer-fn out-task
                                                                        out-tuple
                                                                        overflow-buffer)
                                                           ))
                                         (if rooted?
                                           (do
                                             (.put pending root-id [task-id
                                                                    message-id
                                                                    {:stream out-stream-id :values values}
                                                                    (if (sampler) (System/currentTimeMillis))])
                                             (task/send-unanchored task-data
                                                                   ACKER-INIT-STREAM-ID
                                                                   [root-id (bit-xor-vals out-ids) task-id]
                                                                   overflow-buffer))
                                           (when message-id
                                             (ack-spout-msg executor-data task-data message-id
                                                            {:stream out-stream-id :values values}
                                                            (if (sampler) 0))))
                                         (or out-tasks [])
                                         )
                                       
                                       )]]
          ;;;
          (.put adaptive-timeout-obj (str task-id) end-to-end)
          ;;;
          
          
          (builtin-metrics/register-all (:builtin-metrics task-data) storm-conf (:user-context task-data))
          (builtin-metrics/register-queue-metrics {:sendqueue (:batch-transfer-queue executor-data)
                                                   :receive receive-queue}
                                                  storm-conf (:user-context task-data))

          (.open spout-obj
                 storm-conf
                 (:user-context task-data)
                 (SpoutOutputCollector.
                  (reify ISpoutOutputCollector
                    (^List emit [this ^String stream-id ^List tuple ^Object message-id]
                      (write-message log-spout-msg "Send spout message called at spout\n" component-id)
                      (send-spout-msg stream-id tuple message-id nil)
                      
                      ;;;
                      (try
                      (.on_emit (.get adaptive-timeout-obj (str task-id) ) (str message-id) )
                      (catch Exception e (str "Caught exception " (.getMessage e))))
                      ;;;
                      (write-message log-spout-msg "Send spout message finished\n" component-id)
                      )
                    (^void emitDirect [this ^int out-task-id ^String stream-id
                                       ^List tuple ^Object message-id]
                      
                      (write-message log-spout-msg "Send spout message called at spout\n" component-id)
                      (send-spout-msg stream-id tuple message-id out-task-id)
                      
                      ;;;
                      (try
                      (.on_emit (.get adaptive-timeout-obj (str task-id)) (str message-id) )
                      (catch Exception e (str "Caught exception " (.getMessage e))))
                      ;;;
                      (write-message log-spout-msg "Send spout message finished\n" component-id)
                      
                      )
                    (reportError [this error]
                      (write-message log-spout-msg "Encountered error\n" component-id)
                      (report-error error)
                      )))))
        (reset! open-or-prepare-was-called? true) 
        (log-message "Opened spout " component-id ":" (keys task-datas))
        (setup-metrics! executor-data)
        
        (disruptor/consumer-started! (:receive-queue executor-data))
        (fn []
          ;; This design requires that spouts be non-blocking
          (disruptor/consume-batch receive-queue event-handler)
          
          ;; try to clear the overflow-buffer
          (try-cause
            (while (not (.isEmpty overflow-buffer))
              (let [[out-task out-tuple] (.peek overflow-buffer)]
                (transfer-fn out-task out-tuple false nil)
                (.removeFirst overflow-buffer)))
          (catch InsufficientCapacityException e
            ))
          
          (let [active? @(:storm-active-atom executor-data)
                curr-count (.get emitted-count)]
            (if (and (.isEmpty overflow-buffer)
                     (or (not max-spout-pending)
                         (< (.size pending) max-spout-pending)))
              (if active?
                (do
                  (when-not @last-active
                    (reset! last-active true)
                    (log-message "Activating spout " component-id ":" (keys task-datas))
                    (fast-list-iter [^ISpout spout spouts] (.activate spout)))
               
                  (fast-list-iter [^ISpout spout spouts]
                                  (do
                                    (write-message log-spout-msg "Next tuple called at spout\n" component-id)
                                    (.nextTuple spout)
                                    (write-message log-spout-msg "Next tuple finished at spout\n" component-id)
                                    
                                  )
                                  
                                  ))
                (do
                  (when @last-active
                    (reset! last-active false)
                    (log-message "Deactivating spout " component-id ":" (keys task-datas))
                    (write-message log-spout-msg "Deactivating spout\n" component-id)
                    (fast-list-iter [^ISpout spout spouts] (.deactivate spout)))
                  ;; TODO: log that it's getting throttled
                  (Time/sleep 100)))
                  (write-message log-spout-msg "Wrong part of if\n" component-id)
              )
            (if (and (= curr-count (.get emitted-count)) active?)
              (do (.increment empty-emit-streak)
                  (write-message log-spout-msg "Spout in empty emit streak\n" component-id)
                  (.emptyEmit spout-wait-strategy (.get empty-emit-streak)))
              (do
              (.set empty-emit-streak 0)
              (write-message log-spout-msg "Empty emit streak set reset at spout\n" component-id)
              )
              ))           
          0))
      :kill-fn (:report-error-and-die executor-data)
      :factory? true
      :thread-name component-id)]))

(defn- tuple-time-delta! [^TupleImpl tuple]
  (let [ms (.getProcessSampleStartTime tuple)]
    (if ms
      (time-delta-ms ms))))
      
(defn- tuple-execute-time-delta! [^TupleImpl tuple]
  (let [ms (.getExecuteSampleStartTime tuple)]
    (if ms
      (time-delta-ms ms))))

(defn put-xor! [^Map pending key id]
  (let [curr (or (.get pending key) (long 0))]
    (.put pending key (bit-xor curr id))))

(defmethod mk-threads :bolt [executor-data task-datas]
  (let [execute-sampler (mk-stats-sampler (:storm-conf executor-data))
        executor-stats (:stats executor-data)
        {:keys [storm-conf component-id worker-context transfer-fn report-error sampler
                open-or-prepare-was-called?]} executor-data
        rand (Random. (Utils/secureRandomLong))
        
        
        
        ;;timeout related
        emitted-count (MutableLong. 0)
        comp-start-time (:comp-start-time executor-data)
        curr-mu (MutableObject. (float 0))
        curr-lambda (MutableObject. (float 0))
        start-timestamp(MutableObject. (long 0))
        stop-timestamp (MutableObject. (long 0))
        lamda-computer (:lambda-estimator-fn executor-data)   
        ^HashMap inter-arrival-time-map (:inter-arrival-time-map executor-data)
        ^HashMap queuing-delay-start-map (:queuing-delay-start-map executor-data)
        queuing-delay (MutableObject. (long 0))
        inter-arrival-time (MutableObject. (long 0))
        msg-drop-probability (return-msg-drop-probability (:storm-conf executor-data))
        rand_2 (Random.)
        
        
        tuple-action-fn (fn [task-id ^TupleImpl tuple]
                          ;; synchronization needs to be done with a key provided by this bolt, otherwise:
                          ;; spout 1 sends synchronization (s1), dies, same spout restarts somewhere else, sends synchronization (s2) and incremental update. s2 and update finish before s1 -> lose the incremental update
                          ;; TODO: for state sync, need to first send sync messages in a loop and receive tuples until synchronization
                          ;; buffer other tuples until fully synchronized, then process all of those tuples
                          ;; then go into normal loop
                          ;; spill to disk?
                          ;; could be receiving incremental updates while waiting for sync or even a partial sync because of another failed task
                          ;; should remember sync requests and include a random sync id in the request. drop anything not related to active sync requests
                          ;; or just timeout the sync messages that are coming in until full sync is hit from that task
                          ;; need to drop incremental updates from tasks where waiting for sync. otherwise, buffer the incremental updates
                          ;; TODO: for state sync, need to check if tuple comes from state spout. if so, update state
                          ;; TODO: how to handle incremental updates as well as synchronizations at same time
                          ;; TODO: need to version tuples somehow
                          
                          ;;(log-debug "Received tuple " tuple " at task " task-id)
                          ;; need to do it this way to avoid reflection
                          (let [stream-id (.getSourceStreamId tuple)
                                
                                ^LinkedList base-list  (if (.containsKey queuing-delay-start-map (str task-id))
                                                        (.get queuing-delay-start-map (str task-id))
                                                        (.add (java.util.LinkedList.) (System/nanoTime))
                                                       )
                                ;;q-delay-start (if (> (.size base-list) 0)                                                
                                ;;               (.poll base-list)
                                ;;                (System/nanoTime))
                                ]
                            
                            ;;;
                            ;;(.setObject inter-arrival-time (.get (.get inter-arrival-time-map (str task-id)) 0))
                            ;;(if (not (= q-delay-start nil))
                            ;;  (.setObject queuing-delay (long (- (System/nanoTime) q-delay-start)))
                            ;;  (.setObject queuing-delay (long 0))
                            ;;)
                            (.setObject inter-arrival-time (long (.get (.get inter-arrival-time-map (str task-id)) 0)))
                            (.setObject curr-lambda (float (lamda-computer task-id)))
                            (.setObject start-timestamp (System/nanoTime))
                            ;;;
                            
                            (condp = stream-id
                              Constants/METRICS_TICK_STREAM_ID (metrics-tick executor-data (get task-datas task-id) tuple)
                              (let [task-data (get task-datas task-id)
                                    ^IBolt bolt-obj (:object task-data)
                                    user-context (:user-context task-data)
                                    sampler? (sampler)
                                    execute-sampler? (execute-sampler)
                                    now (if (or sampler? execute-sampler?) (System/currentTimeMillis))]
                                (when sampler?
                                  (.setProcessSampleStartTime tuple now))
                                (when execute-sampler?
                                  (.setExecuteSampleStartTime tuple now))
                                (.execute bolt-obj tuple)
                                ;;;;
                                (.increment emitted-count)
                                (.setObject curr-mu (float (* (/ (float (.get emitted-count)) (time-delta-ms comp-start-time)) 1000)))
                                ;;;;
                                (let [delta (tuple-execute-time-delta! tuple)]
                                  (task/apply-hooks user-context .boltExecute (BoltExecuteInfo. tuple task-id delta))
                                  (when delta
                                    (builtin-metrics/bolt-execute-tuple! (:builtin-metrics task-data)
                                                                         executor-stats
                                                                         (.getSourceComponent tuple)                                                      
                                                                         (.getSourceStreamId tuple)
                                                                         delta)
                                    (stats/bolt-execute-tuple! executor-stats
                                                               (.getSourceComponent tuple)
                                                               (.getSourceStreamId tuple)
                                                               delta)))))))]
    
    ;; TODO: can get any SubscribedState objects out of the context now
    
    
    
    

    [(async-loop
      (fn []
        ;; If topology was started in inactive state, don't call prepare bolt until it's activated first.
        (while (not @(:storm-active-atom executor-data))          
          (Thread/sleep 100))
        
        (log-message "Preparing bolt " component-id ":" (keys task-datas))
        (doseq [[task-id task-data] task-datas
                :let [^IBolt bolt-obj (:object task-data)
                      tasks-fn (:tasks-fn task-data)
                      user-context (:user-context task-data)
                      bolt-emit (fn [stream anchors values task]
                                  
                                  
                                  
                                  
                                  (let [out-tasks (if task
                                                    (tasks-fn task stream values)
                                                    (tasks-fn stream values))]
                                    (fast-list-iter [t out-tasks]
                                                    (let [anchors-to-ids (HashMap.)]
                                                      (fast-list-iter [^TupleImpl a anchors]
                                                                      (let [root-ids (-> a .getMessageId .getAnchorsToIds .keySet)]
                                                                        (when (pos? (count root-ids))
                                                                          (let [edge-id (MessageId/generateId rand)]
                                                                            (.updateAckVal a edge-id)
                                                                            (fast-list-iter [root-id root-ids]
                                                                                            (put-xor! anchors-to-ids root-id edge-id))
                                                      
                                                                            ))))
                                                      
                                                      (if (not (should-it-be-dropped msg-drop-probability rand_2))
                                                      (transfer-fn t
                                                                   (TupleImpl. worker-context
                                                                               values
                                                                               task-id
                                                                               stream
                                                                               (MessageId/makeId anchors-to-ids)))
                                                      )
                                                      
                                                      ))
                                    (or out-tasks [])))]]
          (builtin-metrics/register-all (:builtin-metrics task-data) storm-conf user-context)
          (if (= component-id Constants/SYSTEM_COMPONENT_ID)
            (builtin-metrics/register-queue-metrics {:sendqueue (:batch-transfer-queue executor-data)
                                                     :receive (:receive-queue executor-data)
                                                     :transfer (:transfer-queue (:worker executor-data))}
                                                    storm-conf user-context)
            (builtin-metrics/register-queue-metrics {:sendqueue (:batch-transfer-queue executor-data)
                                                     :receive (:receive-queue executor-data)}
                                                    storm-conf user-context)
            )

          (.prepare bolt-obj
                    storm-conf
                    user-context
                    (OutputCollector.
                     (reify IOutputCollector
                       (emit [this stream anchors values]
                         (bolt-emit stream anchors values nil))
                       (emitDirect [this task stream anchors values]
                         (bolt-emit stream anchors values task))
                       (^void ack [this ^Tuple tuple]
                         (let [^TupleImpl tuple tuple
                               ack-val (.getAckVal tuple)]
                           (.setObject stop-timestamp (System/nanoTime))
                           (fast-map-iter [[root id] (.. tuple getMessageId getAnchorsToIds)]
                                          
                                          (if (not (should-it-be-dropped msg-drop-probability rand_2))
                                           (task/send-unanchored task-data
                                                                 ACKER-ACK-STREAM-ID
                                                                 ;;[root (bit-xor id ack-val) (int (* (.getObject curr-lambda) 1000)) (int (*(.getObject curr-mu) 1000))]
                                                                 [root (bit-xor id ack-val) (.getObject curr-lambda) (.getObject curr-mu) (.getObject start-timestamp) (.getObject stop-timestamp) (.getObject inter-arrival-time) (.getObject queuing-delay)]
                                                                 ;;[root (bit-xor id ack-val) 0 0]
                                                                 )                                          
                                          )
                                          
                                          
                                          ))
                         (let [delta (tuple-time-delta! tuple)]
                           (task/apply-hooks user-context .boltAck (BoltAckInfo. tuple task-id delta))
                           (when delta
                             (builtin-metrics/bolt-acked-tuple! (:builtin-metrics task-data)
                                                                executor-stats
                                                                (.getSourceComponent tuple)                                                      
                                                                (.getSourceStreamId tuple)
                                                                delta)
                             (stats/bolt-acked-tuple! executor-stats
                                                      (.getSourceComponent tuple)
                                                      (.getSourceStreamId tuple)
                                                      delta))))
                       (^void fail [this ^Tuple tuple]
                         (.setObject stop-timestamp (System/nanoTime))
                         (fast-list-iter [root (.. tuple getMessageId getAnchors)]
                                         
                                         (if (not (should-it-be-dropped msg-drop-probability rand_2))
                                          (task/send-unanchored task-data
                                                                ACKER-FAIL-STREAM-ID
                                                                [root (.getObject curr-lambda) (.getObject curr-mu) (.getObject start-timestamp) (.getObject stop-timestamp) (.getObject inter-arrival-time) (.getObject queuing-delay)]
                                                                ;;[root 0 0]
                                                                )
                                         )
                                         
                                         
                                         )
                         (let [delta (tuple-time-delta! tuple)]
                           (task/apply-hooks user-context .boltFail (BoltFailInfo. tuple task-id delta))
                           (when delta
                             (builtin-metrics/bolt-failed-tuple! (:builtin-metrics task-data)
                                                                 executor-stats
                                                                 (.getSourceComponent tuple)                                                      
                                                                 (.getSourceStreamId tuple))
                             (stats/bolt-failed-tuple! executor-stats
                                                       (.getSourceComponent tuple)
                                                       (.getSourceStreamId tuple)
                                                       delta))))
                       (reportError [this error]
                         (report-error error)
                         )))))
        (reset! open-or-prepare-was-called? true)        
        (log-message "Prepared bolt " component-id ":" (keys task-datas))
        (setup-metrics! executor-data)

        (let [receive-queue (:receive-queue executor-data)
              event-handler (mk-task-receiver executor-data tuple-action-fn)]
          (disruptor/consumer-started! receive-queue)
          (fn []            
            (disruptor/consume-batch-when-available receive-queue event-handler)
            0)))
      :kill-fn (:report-error-and-die executor-data)
      :factory? true
      :thread-name component-id)]))

(defmethod close-component :spout [executor-data spout]
  (.close spout))

(defmethod close-component :bolt [executor-data bolt]
  (.cleanup bolt))

;; TODO: refactor this to be part of an executor-specific map
(defmethod mk-executor-stats :spout [_ rate]
  (stats/mk-spout-stats rate))

(defmethod mk-executor-stats :bolt [_ rate]
  (stats/mk-bolt-stats rate))

