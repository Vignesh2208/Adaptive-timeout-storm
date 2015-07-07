/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.lang.Object;
import java.util.ArrayList;

import backtype.storm.Constants;
import backtype.storm.coordination.CoordinatedBolt;
import backtype.storm.tuple.Values;
import clojure.lang.RT;


public class End_to_End {
	
	
    public String TIMEOUT_BASE_DIR;
    public String Component_ID;
    public String Task_ID;
    public String Topology_name;
    public String Topology_info;
    
    int no_of_acks = 0;
	int no_of_fails = 0;
	int no_of_emitted_tuples = 0;
	String _latencyPath;
	String _total_latencyPath;
	String Statistics_file_path;
	String _timeOutPath;
	String _pythonScript;
	String log_file;
	double latency;
	int tickTupleNo;
	String mode;
	
	String _baseDir;
	
	Map<String, Long> _startTime = new HashMap<String, Long>(); //Stores the emit time of tuple
	Map<String, Long> _total_Time = new HashMap<String, Long>(); //Stores the emit time of tuple

    
    public End_to_End(String TIMEOUT_BASE_DIR, String Component_ID, String Task_ID, String Topology_name, String Topology_info,String mode){
    	
    	this.TIMEOUT_BASE_DIR = TIMEOUT_BASE_DIR;
    	this.Component_ID = Component_ID;
    	this.Topology_name = Topology_name;
    	this.Topology_info = Topology_info;
    	this.Task_ID = Task_ID;
    	this.mode = mode;
    	
    	this._baseDir = TIMEOUT_BASE_DIR + Topology_name + "/";
    	this._latencyPath = this._baseDir + Topology_info + "/" + Task_ID + "-" + Component_ID + "_latency_" + Topology_info + ".txt";
    	this._total_latencyPath = this._baseDir + Topology_info + "/" + Task_ID + "-" + Component_ID + "_total_latency_" + Topology_info + ".txt";
    	this.Statistics_file_path = this._baseDir + Topology_info + "/" + Task_ID + "-" + Component_ID + "_statistics_" + Topology_info + ".txt";
    	this._pythonScript = TIMEOUT_BASE_DIR + "timeout_compute.py";
    	this._timeOutPath = TIMEOUT_BASE_DIR + Component_ID + "-" + Task_ID + ".txt";
    	this.log_file = TIMEOUT_BASE_DIR + "log_tuple_action.txt";
    	
    	
    	/*
    	 * Logging file locations : 
    	 * 
    	 * TIMEOUT_BASE_DIR : (defined in Constants.java) /app/home/storm/
    	 * Base Dir : TIMEOUT_BASE_DIR/Topology_name/
    	 * temporary per tick tuple latency file :  TIMEOUT_BASE_DIR/Topology_name/Topology_info/<(Task_ID)-(Compnent_ID)_latency_(Topology_info)>.txt
    	 * Statistics file :  TIMEOUT_BASE_DIR/Topology_name/Topology_info/<(Task_ID)-(Compnent_ID)_statistics_(Topology_info)>.txt
    	 * Total latency file : (for experimental evalutation) 	: TIMEOUT_BASE_DIR/Topology_name/Topology_info/<(Task_ID)-(Compnent_ID)_total_latency_(Topology_info)>.txt
    	 * Python script : TIMEOUT_BASE_DIR/timeout_compute.py
    	 * Debug log files : TIMEOUT_BASE_DIR/log_tuple_action.txt ; TIMEOUT_BASE_DIR/log_spout_msg.txt ; TIMEOUT_BASE_DIR/log.txt ; TIMEOUT_BASE_DIR/log_acker_action.txt
    	 * Timeout value file : (contains appended timeout values for evaluation purposes ) : TIMEOUT_BASE_DIR/<(Component_ID)-(Task_ID)>.txt
    	 *  
    	 */
    	
    	if(mode.equals("END_TO_END")){
    	
    		try {
 	        	File file = new File(_latencyPath + "." + tickTupleNo);
 	        	file.getParentFile().mkdirs(); 
 	        	PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file, false)));
 	        	out.println("#");
 	        	out.close();
 	    	} catch (IOException e) {
 	        
 	    	}
    	}
    	
    }
    /*
     * Called by spout on each emit. The start time is noted if it is a new tuple.
     */
    public void on_emit(String hash){
    	if(mode.equals("END_TO_END")|| mode.equals("NORMAL")||mode.startsWith("QUEUEING MODEL")){
    		Long time_stamp = System.nanoTime();
    		_startTime.put(hash, time_stamp);
    		if(_total_Time.get(hash) == null){
    			_total_Time.put(hash, time_stamp);
    		}
    		else{
    			//System.out.println("Time stamp already there = " + _total_Time.get(hash));
    		}
    	
	    	no_of_emitted_tuples ++;
    	}
    	
    }
    
    //currently not called
    public void on_NextTuple(){
    	if(mode.equals("END_TO_END") || mode.equals("NORMAL")|| mode.startsWith("QUEUEING MODEL")){
    		int rem = no_of_emitted_tuples % 2;
    		File statistics_file_ptr = new File(Statistics_file_path + rem);
        	statistics_file_ptr.getParentFile().mkdirs(); //Created the necessary folders in case the parent directories are absent
    		PrintWriter statistics_out_ptr;
    	
			try {
				statistics_out_ptr = new PrintWriter(new BufferedWriter(new FileWriter(statistics_file_ptr, false)));
				statistics_out_ptr.println("No of acks = " + no_of_acks); //logs in milli-seconds
	        	statistics_out_ptr.println("No of fails = " + no_of_fails); 
	        	statistics_out_ptr.println("No of emitted tuples = " + no_of_emitted_tuples); 
	        	statistics_out_ptr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    }
    /*
     * Called by spout on each ACK. If mode is END_TO_END, the process time is appended to the latency file. Tuple statistics are updated.
     */
    
    public void on_Ack(Object id){
    	  
		  if(mode.equals("END_TO_END") || mode.equals("NORMAL")||mode.startsWith("QUEUEING MODEL")){
			  
			  Long value = _startTime.get(id);
			  if(value != null){
				  _startTime.remove(id);

				  no_of_acks++;
				  if(mode.equals("END_TO_END")){
				    	try {
				    		File file = new File(_latencyPath + "." + tickTupleNo);
				    		file.getParentFile().mkdirs(); //Created the necessary folders in case the parent directories are absent
				    		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));// true for append
				        	out.println((System.nanoTime() - value)/Math.pow(10,6)); //logs in milli-seconds
				        	out.close();
				    	} catch (Exception e) {
					    	
				    	}
				  }
				    	
			  }
		  }
		 if(mode.equals("END_TO_END") || mode.equals("NORMAL")||mode.startsWith("QUEUEING MODEL")){
			 int rem = no_of_emitted_tuples % 2;
			    	try{
			        	File statistics_file_ptr = new File(Statistics_file_path + rem);
			        	statistics_file_ptr.getParentFile().mkdirs(); //Created the necessary folders in case the parent directories are absent
			    		PrintWriter statistics_out_ptr = new PrintWriter(new BufferedWriter(new FileWriter(statistics_file_ptr, false)));// true for append
			        	statistics_out_ptr.println("No of acks = " + no_of_acks); //logs in milli-seconds
			        	statistics_out_ptr.println("No of fails = " + no_of_fails); 
			        	statistics_out_ptr.println("No of emitted tuples = " + no_of_emitted_tuples); 
			        	statistics_out_ptr.close();
			    	} catch (Exception e) {
				    	
			    	}
				        
				        
				    	
				    	
			  	
		  
		  
			Long new_value = _total_Time.get(id);
		  	if(new_value != null){
			  	try {
				  		File file_ptr = new File(_total_latencyPath);
				  		file_ptr.getParentFile().mkdirs(); //Created the necessary folders in case the parent directories are absent
			    		PrintWriter out_ptr = new PrintWriter(new BufferedWriter(new FileWriter(file_ptr, true)));// true for append
			        	out_ptr.println((System.nanoTime() - new_value)/Math.pow(10,6)); //logs in milli-seconds
			        	out_ptr.close();
				  
			  	} catch (IOException e) {
				  
			  	}
			  	_total_Time.remove(id);
		  	}
    	
		}
    }
    /*
     * Called by spout on receiving a tick tuple. It calls a python script to operate on the previous latency file to determine the timeout value
     * assuming Johnson SU distribution. A new latency file is created for the next tuple.
     */
    public void on_Tick_tuple(Object id){
    	
    	File file_ptr;
    	PrintWriter out_ptr;
    	// Assuming Johnson's SU distribution
    	if(mode.equals("END_TO_END")){
    		System.out.println("Initiated timeout computation...");    		
    		
    		try {
    			Runtime.getRuntime().exec("python " + _pythonScript + " " + _latencyPath + "." + tickTupleNo + " " + _timeOutPath + " " + Topology_info + " " + _baseDir);
    			
    		} catch (Exception e) {			
    		}
    		
    		
    		tickTupleNo = tickTupleNo + 1;
    		try {
    			File file = new File(_latencyPath + "." + tickTupleNo);
    			file.getParentFile().mkdirs(); 
    			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file, false)));// true for append
    			out.println("#"); 
    			out.close();
    		} catch (Exception e) {   
    		}
    		
    		
    		
    	}
    }
    
    /*
     * Called by spout when a tuple is failed. Update metrics such as number of failed tuples, number of acked tuples and number of emitted tuples.
     * These statistics are written to the statistics file specfied by the statistics file path. If the mode is END_TO_END, the process time of the
     * tuple is also logged in the latency file of the curret tick tuple.
     * 
     */
    public void on_Fail(Object id){
    	
    	 
    	 if(mode.equals("END_TO_END") || mode.equals("NORMAL")||mode.startsWith("QUEUEING MODEL")){
    		 int rem = no_of_emitted_tuples % 2;
    		Long value = _startTime.get(id);
    		
		  	if(value != null){
			  	
				  	no_of_fails++;
				  	Long time_stamp = System.nanoTime();
				  	if(mode.equals("END_TO_END")){
				  	try {
				  		File file = new File(_latencyPath + "." + tickTupleNo);
				  		file.getParentFile().mkdirs(); //Created the necessary folders in case the parent directories are absent
			    		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));// true for append
			        	out.println((time_stamp - value)/Math.pow(10,6)); //logs in milli-seconds
			        	out.close();
				  	}
					catch (Exception e) {				        
		        	}
				  	}
				  	if(mode.equals("END_TO_END") || mode.equals("NORMAL")||mode.startsWith("QUEUEING MODEL")){ 
				  		try{
				  			File statistics_file_ptr = new File(Statistics_file_path + rem);
				  			statistics_file_ptr.getParentFile().mkdirs(); //Created the necessary folders in case the parent directories are absent
				  			PrintWriter statistics_out_ptr = new PrintWriter(new BufferedWriter(new FileWriter(statistics_file_ptr, false)));// true for append
				  			statistics_out_ptr.println("No of acks = " + no_of_acks); //logs in milli-seconds
				  			statistics_out_ptr.println("No of fails = " + no_of_fails); 
				  			statistics_out_ptr.println("No of emitted tuples = " + no_of_emitted_tuples); 
				  			statistics_out_ptr.close();
				  		}
				  		catch (Exception e) {				        
				  		}
				  	
				  
				  	_startTime.put((String) id, time_stamp);
				
				  	}			  
			  		
			  
		  	}
    	 }
    	
    }
}
    
