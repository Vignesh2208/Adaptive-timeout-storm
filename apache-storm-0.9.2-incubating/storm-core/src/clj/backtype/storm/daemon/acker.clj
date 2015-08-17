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
(ns backtype.storm.daemon.acker
  (:import [backtype.storm.task OutputCollector TopologyContext IBolt])
  (:import [backtype.storm.tuple Tuple Fields])
  (:import [backtype.storm.utils RotatingMap MutableObject MutableLong])
  (:import [java.util List HashMap Map ArrayList Random])
  (:import [backtype.storm.utils End_to_End Queueing_model])
  (:import [backtype.storm Config])
  (:import [backtype.storm Constants])
  (import [java.io BufferedWriter FileWriter File])
  (:use [backtype.storm config util log])
  (:gen-class
   :init init
   :implements [backtype.storm.task.IBolt]
   :constructors {[] []}
   :state state ))

(def ACKER-COMPONENT-ID "__acker")
(def ACKER-INIT-STREAM-ID "__ack_init")
(def ACKER-ACK-STREAM-ID "__ack_ack")
(def ACKER-FAIL-STREAM-ID "__ack_fail")
(def ACKER-TIMEOUT-DISPATCH-STREAM-ID "__ack_time_out")


(def acker-tuple_action (str Constants/TIMEOUT_FILE_BASE_DIR "debug/log_acker.txt"))
(def is-debug-enabled (MutableObject. (int 0)))
(def is-debug-log-enabled (MutableObject. (int 0)))
;;(def task-id  (MutableObject.(int 0)))

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

(defn debug-message [filename message id]
  (if (= (.getObject is-debug-log-enabled) 1)
   (spit filename (str "Acker ID : " id ", msg :" message) :append true)
  )
)

(defn init-log-files []
  (if (= (.getObject is-debug-log-enabled) 1)
    (do
      (.mkdirs (File. (str Constants/TIMEOUT_FILE_BASE_DIR "debug")))

      (with-open [wrt (clojure.java.io/writer acker-tuple_action)]
      (.write wrt "##### New acker-tuple-action-log #####\n"))
   )
    
  )
)


(defn is-the-adaptive-timeout-enabled [^Map conf]
  (if (.containsKey conf Config/ADAPTIVE_TIMEOUT_ENABLED) 
    (if (.get conf Config/ADAPTIVE_TIMEOUT_ENABLED)
      true
      false
    )
    false
  )
)  

(defn obtain-adaptive-timeout-mode [^Map conf]
  (if (is-the-adaptive-timeout-enabled conf)
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




(def locking-object (Object.))
(def MAX_TASK_RATE_LIST_SIZE 1000)

(defn- update-ack [curr-entry val]
  (let [old (get curr-entry :val 0)]
    
    (assoc curr-entry :val (bit-xor old val))
    ;; 
    ))

(defn- acker-emit-direct [^OutputCollector collector ^Integer task ^String stream ^List values]
  (.emitDirect collector task stream values)
  )

(defn- update-task-rate-map [^HashMap task-rate-map lambda mu service-time inter-arrival-time task-id ^HashMap task-count-map]
 (if (.containsKey task-rate-map (str task-id))
 (do
  (let [ new-entry (java.util.ArrayList.) 
        ^ArrayList base-list (.get task-rate-map (str task-id))
       ]
   (.add new-entry lambda )
   (.add new-entry mu )
   (.add new-entry service-time )
   (.add new-entry inter-arrival-time )
   (if (< (.size base-list) MAX_TASK_RATE_LIST_SIZE )
     (do
       (.add base-list new-entry)
       (.put task-count-map (str task-id) (+ (.get task-count-map (str task-id)) 1))
       
     )
     (do
       (let [index (rem (.get task-count-map (str task-id)) (int MAX_TASK_RATE_LIST_SIZE))]   
         (.set base-list index new-entry)
         (.put task-count-map (str task-id) (+ (int (.get task-count-map (str task-id))) 1))
       )
     )
   )
  )
 )
 (do
  (let [new-entry (java.util.ArrayList.)
       new-base-list (java.util.ArrayList.)
       ]
   (.add new-entry lambda )
   (.add new-entry mu )
   (.add new-entry service-time )
   (.add new-entry inter-arrival-time )
   (.add new-base-list new-entry)
   (.put task-rate-map (str task-id) new-base-list)
   (.put task-count-map (str task-id) (int 1))
       
  )
      
 )
    
 )
 
)

(defn- reset-task-rate-map [^HashMap task-rate-map]
 (fast-map-iter [[task-id data] task-rate-map]
   (let [new-base-list (java.util.ArrayList.)]
    (.put task-rate-map (str task-id) new-base-list) 
   )
 )
)

(defn- reset-task-count-map [^HashMap task-count-map]
 (fast-map-iter [[task-id data] task-count-map]
   (.put task-count-map (str task-id) 0) 
  )
)

(defn mk-acker-bolt []
  (let [output-collector (MutableObject.)
        pending (MutableObject.)
        task-rate-map (java.util.HashMap.)
        task-count-map (java.util.HashMap.)
        task-timestamp-map (java.util.HashMap.)
        spout_task_list (java.util.ArrayList.)
        prev-time-out (MutableLong. 30)
        msg-drop-probability (MutableObject.)
        task-id  (MutableObject.(int 0))
        rand_2 (Random.)
        adaptive-timeout-mode (MutableObject.) 
        topology-name (MutableObject.) 
        topology-info (MutableObject.)
        
        queuing-model (MutableObject.)
        timeout-value (MutableObject.(int 100))       
        ]
    (reify IBolt
      (^void prepare [this ^Map storm-conf ^TopologyContext context ^OutputCollector collector]
               (.setObject output-collector collector)
               (.setObject pending (RotatingMap. 2))
               (.setObject task-id (int (.getThisTaskId context)))
               (debug-print "Acker: Task-id is " (str (.getObject task-id)))
               ;;;  FAULT-INJECTOR
               (.setObject msg-drop-probability (return-msg-drop-probability storm-conf))
               (.setObject adaptive-timeout-mode (obtain-adaptive-timeout-mode storm-conf))
               (.setObject topology-name (return-base-dir-name storm-conf))
               (.setObject topology-info (return-topology-specific-info storm-conf))
               (set-debug-enabled storm-conf)
               (set-debug-log-enabled storm-conf)
               (init-log-files)
               (.setObject queuing-model (Queueing_model. context (.getObject topology-name) (.getObject topology-info) (obtain-adaptive-timeout-mode storm-conf) (.getObject is-debug-log-enabled) ))
               ;;;
               
               )
      (^void execute [this ^Tuple tuple]
             (let [^RotatingMap pending (.getObject pending)
                   stream-id (.getSourceStreamId tuple)]
               
               
               (if (= stream-id Constants/SYSTEM_TICK_STREAM_ID)
                 (do
                   
                   (debug-print (str "Acker: Received tick tuple" ))
                   (let [
                         queue-model-obj (.getObject queuing-model)                         
                        ]
                     
                     ;;Send out the computed new timeout value to spouts only if the QUEUING MODEL based timeout mechanism is enabled
                     (if (boolean (re-find #"QUEUEING MODEL" (.getObject adaptive-timeout-mode))) ;; if QUEUING MODEL substring is present in mode
                      (do
                        
                        (.set_task_rate_map queue-model-obj task-rate-map)                      
                        (.setObject timeout-value (.on_Tick_tuple queue-model-obj))          
                        (debug-message acker-tuple_action (str "QUEUING MODEL Timeout = " (.getObject timeout-value) "\n") (.getObject task-id))
                         
                        (fast-list-iter [task-id spout_task_list]
                            (acker-emit-direct   (.getObject output-collector)
                                                 task-id
                                                 ACKER-TIMEOUT-DISPATCH-STREAM-ID
                                                [(.getObject timeout-value)]
                                                 )           
                         )
                        (.setObject timeout-value (int 100)) ;; For the acker it always remains the same
                     )
                     ;;;; else
                     (if (= (.getObject adaptive-timeout-mode) "END_TO_END")
                       (.setObject timeout-value (int 100))
                       ;;(.setObject timeout-value (int 30))                        
                     )
                     )
                     
                      (if (not= (.getObject adaptive-timeout-mode) "NORMAL")                        
                      (with-open [w (clojure.java.io/writer (str Constants/TIMEOUT_FILE_BASE_DIR ACKER-COMPONENT-ID "-" (str (.getObject task-id)) ".txt"))]
                      (.write w (str (.getObject timeout-value))))
                      )
                      
                      (reset-task-rate-map task-rate-map)
                      (reset-task-count-map task-count-map) 
                      
                   )
                 )
                 (let [id (.getValue tuple 0)
                       ^OutputCollector output-collector (.getObject output-collector)
                       curr (.get pending id)
                       curr (condp = stream-id
                                ACKER-INIT-STREAM-ID (do
                                                     (.add spout_task_list (.getSourceTask tuple))
                                                       
                                                     (-> curr
                                                         (update-ack (.getValue tuple 1))
                                                         (assoc :spout-task (.getValue tuple 2)))
                                                     )
                                ACKER-ACK-STREAM-ID (do
                                                     ;; For some reason, cannot print the tuple values 
                                                     ;;(println (str "Acker : Source service time : " (long (- (.getValue tuple 5) (.getValue 4))) " Source inter-arrivaltime : " (long (.getValue tuple 6)) "\n"))
                                                     (update-task-rate-map task-rate-map (.getValue tuple 2) (.getValue tuple 3) (- (.getValue tuple 5) (.getValue tuple 4)) (.getValue tuple 6) (.getSourceTask tuple) task-count-map)
                                                     ;;(debug-message acker-tuple_action (str "ACK Received \n") (.getObject task-id))
                                                     (update-ack curr (.getValue tuple 1))
                                                     
                                                    )
                                ACKER-FAIL-STREAM-ID (do
                                                      (update-task-rate-map task-rate-map (.getValue tuple 1) (.getValue tuple 2) (- (.getValue tuple 4) (.getValue tuple 3)) (.getValue tuple 5) (.getSourceTask tuple) task-count-map)
                                                      (assoc curr :failed true)
                                                      )
                                                     
                               Constants/SYSTEM_TIMEOUT_STREAM_ID (do 
                                                                  
                                                                   (debug-print (str "Timeout expired tuple received at " ACKER-COMPONENT-ID ))
                                                                   (.rotate pending)
                                                                  )
                               Constants/SPOUT_COMPUTED_TIMEOUT_STREAM_ID (do
                                                                            
                                                                            (let [recv-timeout-value (.getValue tuple 0)]
                                                                                 (if (>=  recv-timeout-value (.get prev-time-out))
                                                                                     (do
                                                                                     (debug-print (str "Acker : Received a greater spout computed timeout value. Assigning new timeout value .."))
                                                                                     (.set prev-time-out recv-timeout-value)
                                                                                     (with-open [w (clojure.java.io/writer (str Constants/TIMEOUT_FILE_BASE_DIR ACKER-COMPONENT-ID "-" (str (.getObject task-id)) ".txt"))]
                                                                                       (.write w (str recv-timeout-value)))
                                                                                     
                                                                                     )
                                                                                 )
                                                                            )
                                                                          )
                                )
                                ]
                                
                   (.put pending id curr)
                   (when (and curr (:spout-task curr))
                     (cond (= 0 (:val curr))
                           (do
                             (.remove pending id)
                             (debug-message acker-tuple_action (str "Checking if ACK should be dropped [experimental]\n") (.getObject task-id))
                             (try
                               (if (not (should-it-be-dropped (.getObject msg-drop-probability) rand_2))
                                 (do
                                   (acker-emit-direct output-collector
                                                      (:spout-task curr)
                                                      ACKER-ACK-STREAM-ID
                                                      [id]
                                                     )
                                   (debug-message acker-tuple_action (str "ACK not dropped\n") (.getObject task-id)) 
                                 )
                                 (debug-message acker-tuple_action (str "ACK dropped\n") (.getObject task-id))                   
                                )
                               
                             (catch Exception e (str "Caught exception " (.getMessage e))))
                             
                             
                             )
                           (:failed curr)
                           (do
                             (.remove pending id)
                             (debug-message acker-tuple_action (str "Checking if Fail should be dropped [experimental]\n") (.getObject task-id))
                             
                             (try
                               (if (not (should-it-be-dropped (.getObject msg-drop-probability) rand_2))
                                 (do
                                   (acker-emit-direct output-collector
                                                      (:spout-task curr)
                                                      ACKER-FAIL-STREAM-ID
                                                      [id]
                                                      )
                                   (debug-message acker-tuple_action (str "Fail not dropped\n") (.getObject task-id) )              
                                  )
                                  (debug-message acker-tuple_action (str "Fail dropped\n") (.getObject task-id) )                   
                                )                             
                               (catch Exception e (str "Caught exception " (.getMessage e))))
                             
                            )
                           ))
                   
                   (.ack output-collector tuple)
                   )
                 
                 
                 )
               
               ))
      (^void cleanup [this]
        (debug-print (str "Acker: Cleanup"))
        )
      )))

(defn -init []
  [[] (container)])

(defn -prepare [this conf context collector]
  (let [^IBolt ret (mk-acker-bolt)]
    (container-set! (.state ^backtype.storm.daemon.acker this) ret)
    (.prepare ret conf context collector)
    ))

(defn -execute [this tuple]
  (let [^IBolt delegate (container-get (.state ^backtype.storm.daemon.acker this))]
    (.execute delegate tuple)
    ))

(defn -cleanup [this]
  (let [^IBolt delegate (container-get (.state ^backtype.storm.daemon.acker this))]
    (.cleanup delegate)
    ))

