package backtype.storm.utils;



/*import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.lang.Object;
import java.util.ArrayList;
import java.util.Set;
import java.util.List;*/
import java.util.*;
import java.lang.*;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import backtype.storm.Constants;
import backtype.storm.coordination.CoordinatedBolt;
import backtype.storm.tuple.Values;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.StormTopology;
import clojure.lang.RT;

public class Queueing_model {
	
	
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
	int mm1_timeout;
	int ht_timeout;
	
	Map<Integer, String> Task_to_Component_map = new HashMap<Integer,String>();
	Set<String> ComponentIds = new HashSet<String>();
	Set<String> Temp_Component_streams = new HashSet<String>();
	Map<GlobalStreamId,Grouping> Temp_map = new HashMap<GlobalStreamId,Grouping>();
	List<String> longest_path = new ArrayList();
	List<String> heaviest_path = new ArrayList();
	List<String> top_sort_comp_ids = new ArrayList();
	String mode;
	String Topology_name;
	String Topology_info;
	int Log_enabled;
	
	Map<String,List<String>> Component_to_Input_stream_list_map  = new HashMap<String,List<String>>();
	Map<String,List<String>> Component_to_Output_stream_list_map = new HashMap<String,List<String>>();
	
	Map<String,ArrayList<ArrayList>> task_rate_map = new HashMap<String,ArrayList<ArrayList>>();
	Map<String,ArrayList<ArrayList>> task_timestamp_map = new HashMap<String,ArrayList<ArrayList>>();
	
	// For building a Topology graph.
	public static class Node{
		
		String node_id;
		ArrayList<String> out_neighbours;
		ArrayList<String> in_neighbours;
		float weight;
		float heaviest_path;
		int len_longest_path;
		String prev_node;
		
		public Node(String node_id){
			node_id = node_id;
			weight = (float)1.0;
			len_longest_path = 0;
			heaviest_path = (float) 0.0;
			prev_node = "";
			out_neighbours = new ArrayList<String>();
			in_neighbours = new ArrayList<String>();
		}
		
				
		public void add_in_neighbour(String in_neighbour){
			this.in_neighbours.add(in_neighbour);
			
		}
		
		public void add_out_neighbour(String out_neighbour){
			this.out_neighbours.add(out_neighbour);
		}
		
		public void set_weight(float weight){
			this.weight = weight;
		}
		
		public void set_len_longest_path(int len){
			this.len_longest_path = len;
		}
		
		public void set_heaviest_path(float heaviest_path_weight){
			this.heaviest_path = heaviest_path_weight;
		}
		public void set_prev_node_in_longest_path(String prev_node){
			this.prev_node = prev_node;
		}
		
	}
	
	HashMap<String, Node> graph = new HashMap<String, Node>();
	
	
	
	/*
	 * Depth first search of graph
	 */
	static void dfs(HashMap<String,Node> graph, Map<String,Integer> visited, List<String> res, String u) {
	    visited.put(u,1);
	    int i = 0;
	    for (i = 0; i <  graph.get(u).out_neighbours.size(); i++){
	      if (visited.get(graph.get(u).out_neighbours.get(i)) == 0)
	        dfs(graph, visited, res, graph.get(u).out_neighbours.get(i));
	    }
	    res.add(u);
	}
	
	/*
	 * Topological Sort using DFS.
	 */
	 public static List<String> topologicalSort(HashMap<String,Node> graph) {
	    int no_of_nodes = 0;
	    Map<String,Integer> visited = new HashMap<String,Integer>();
	    List<String> res = new ArrayList();
		    
		    
	    for (Map.Entry<String,Node> entry : graph.entrySet())
		{
		    visited.put(entry.getKey(),0);
	    	no_of_nodes++;
	    	
		}
		    
	    for (Map.Entry<String,Node> entry : graph.entrySet())
		{
	    	if(visited.get(entry.getKey()) == 0)
	    		dfs(graph,visited,res,entry.getKey());
		    	
		    	
		}
		    
		    
	    Collections.reverse(res);
	    return res;
	  }

		  
	
	
	
	/*
	 * Constructor. Get list of input and output streams for each component. Build graph.
	 */
	public Queueing_model(TopologyContext context, String Topology_name, String Topology_info,String mode,int Log_enabled){
		
		
		Task_to_Component_map = context. getTaskToComponent();
		ComponentIds = context.getComponentIds();
		this.mode = mode;
		this.Topology_info = Topology_info;
		this.Topology_name = Topology_name;
		this.Log_enabled = Log_enabled;
		
		this.mm1_timeout = Constants.MAX_TIMEOUT;
		this.ht_timeout =  Constants.MAX_TIMEOUT;
		
		String streamId;
		
		for (String componentId : ComponentIds) {
			List<String> input_stream_list = new ArrayList<String>();
			List<String> output_stream_list = new ArrayList<String>();
		    Temp_map = context.getSources(componentId);
		    Temp_Component_streams = context.getComponentStreams(componentId);
		    
		    for (Map.Entry<GlobalStreamId,Grouping> entry : Temp_map.entrySet())
			{
			    streamId = entry.getKey().toString();
		    	input_stream_list.add(streamId);
		    	
			}
		    
		    for (String declared_stream_id : Temp_Component_streams) {
		    	GlobalStreamId g_id = new GlobalStreamId(componentId,declared_stream_id);
		        output_stream_list.add(g_id.toString());
		    }
		    
		    Component_to_Input_stream_list_map.put(componentId,input_stream_list);
		    Component_to_Output_stream_list_map.put(componentId,output_stream_list);
		   
		}
		build_graph();
		
		
		
	}
	
	/*
	 * Checks if the list of in_streams and out_streams have an intersection.
	 */
	public int is_connected (List<String> in_stream, List<String> out_stream){
		
		int i,j;
		for( i = 0; i < in_stream.size(); i++){
			for(j = 0; j < out_stream.size(); j++){
				if(in_stream.get(i).equals(out_stream.get(j))){
					return 1;
				}
			}
		}
		return 0;
	}
	
	/*
	 * Graph is built by identifying which components are connected. It is then topologically sorted to find the longest path.
	 */
	public void build_graph(){
		
		int edge_exists_i_to_j = 0;
		for (String componentId : ComponentIds) {
			Node u = new Node(componentId);
			graph.put(componentId,u);
		}
		
		for(String i : ComponentIds){			
			for (String j : ComponentIds) {
				edge_exists_i_to_j = 0;
				if(!i.equals(j)){
					edge_exists_i_to_j = is_connected(Component_to_Input_stream_list_map.get(j),Component_to_Output_stream_list_map.get(i));
					if(edge_exists_i_to_j == 1){
						graph.get(i).add_out_neighbour(j);
						graph.get(j).add_in_neighbour(i);
					}
					
					
				}
				
			}
			
		}
		
		
		
		top_sort_comp_ids = topologicalSort(graph);		
		find_longest_path();
		
		
		
	}
	
	
	/*
	 * Text book longest path algorithm.
	 */
	public void find_longest_path(){
		
		int max = 0;
		int max_len_value = -1;
		String sink_in_llp = "";
		for(int i = 0 ; i < top_sort_comp_ids.size(); i++){
			String comp_id = top_sort_comp_ids.get(i);
			String prev_node = "";
			max = -1;
			for(int j = 0; j < graph.get(comp_id).in_neighbours.size(); j++){
				String in_neighbour = graph.get(comp_id).in_neighbours.get(j);
				if(graph.get(in_neighbour).len_longest_path > max){
					max = graph.get(in_neighbour).len_longest_path;
					prev_node = in_neighbour;
					
				}
			}
			if(max != -1){
				graph.get(comp_id).set_len_longest_path(max + 1);
				graph.get(comp_id).set_prev_node_in_longest_path(prev_node);
				
			}
			
			if(graph.get(comp_id).len_longest_path > max_len_value){
				max_len_value = graph.get(comp_id).len_longest_path;
				sink_in_llp = comp_id;
			}
			
			
		}// end of for
		
		while(!sink_in_llp.isEmpty()){
			longest_path.add(sink_in_llp);
			sink_in_llp = graph.get(sink_in_llp).prev_node;
		}
		Collections.reverse(longest_path);
		
		
		
	}
	
	
	// Called by acker once it receives a tick tuple. The task rate map is set. 
	//Task rate map : keys : Task-Id, Values : Arraylist of lambda-mu-service time-inter arrival time quadruples
	public void set_task_rate_map(HashMap<String,ArrayList<ArrayList>> task_rate){
		task_rate_map = task_rate;
		    
		
	}
	
	// Called by acker to set the task timestamp map. Task timestamp map contains a list of (tuple-id, process-start-time-ns,process-finish-time-ns) triplets which are all Long types
	// Task timestamp map can be used to fit the process time distribution as well as network latency distribution. The keys are Task-Ids.
	// Currently not used.
	public void set_task_timestamp_map(HashMap<String,ArrayList<ArrayList>> task_timestamp){
		task_timestamp_map = task_timestamp;
		   
		
	}
	
	
	// Called only at the acker when it receives a tick tuple. Tick tuple period = timeout recomputation period
	// Should Return the computed timeout value based on the queue model
	public int on_Tick_tuple(){

		// Do the timeout computation here with the task rate map and the longest path/heaviest_path and Task to component map.
		
				
		System.out.println("Acker: Called on_Tick_Tuple");
		
		if(task_rate_map != null){
			
			if(mode.equals("QUEUEING MODEL Heavy Traffic")){

				ht_timeout = compute_heavy_traffic_approximation();
				return ht_timeout;
			}
			else{
				
				if(mode.equals("QUEUEING MODEL MM1")){
					MM1_approximation();
					return this.mm1_timeout;
				}
				else{
					return Constants.MAX_TIMEOUT;
				}
			}
			
			
		}
		else{
			System.out.println("Error : Task rate map is null. Returning default timeout value.");
		}
		
		
		return Constants.MAX_TIMEOUT; //Default timeout value 
	}
	
	
	public void set_node_weights(HashMap<String,Float> Comp_id_to_weight){
		for (String componentId : ComponentIds) {
			
			graph.get(componentId).set_weight(Comp_id_to_weight.get(componentId));
			graph.get(componentId).set_heaviest_path(Comp_id_to_weight.get(componentId));
		}
	}
	
	/*
	 * Text book heaviest weighted path problem.
	 */
	public void find_heaviest_weighted_path(){
		
		
				float max = (float)0.0;
				float max_wt_value = (float)-1.0;
				String sink_in_hp = "";
				for(int i = 0 ; i < top_sort_comp_ids.size(); i++){
					String comp_id = top_sort_comp_ids.get(i);
					String prev_node = "";
					max = (float)-1.0;
					for(int j = 0; j < graph.get(comp_id).in_neighbours.size(); j++){
						String in_neighbour = graph.get(comp_id).in_neighbours.get(j);
						if( graph.get(in_neighbour).heaviest_path > max){
							max = graph.get(in_neighbour).heaviest_path;
							prev_node = in_neighbour;
							
						}
					}
					if(max != (float)-1.0){
						graph.get(comp_id).set_heaviest_path(graph.get(comp_id).weight + max + (float)1.0 + Constants.NETWORK_DELAY);
						graph.get(comp_id).set_prev_node_in_longest_path(prev_node);
						
					}
					
					if(graph.get(comp_id).heaviest_path > max_wt_value){
						max_wt_value = graph.get(comp_id).heaviest_path;
						sink_in_hp = comp_id;
					}
					
					
				}// end of for
				if(mode.equals("QUEUEING MODEL MM1"))
					this.mm1_timeout = (int)max_wt_value;
				
				heaviest_path = new ArrayList();
				while(!sink_in_hp.isEmpty()){
					heaviest_path.add(sink_in_hp);
					sink_in_hp = graph.get(sink_in_hp).prev_node;
				}
				Collections.reverse(heaviest_path);
				
		
		
	}
	
	public double return_mean(ArrayList<Long> samples){
		double curr_sum;
		curr_sum = 0.0;
		for(int i = 0; i < samples.size(); i++){
			curr_sum = curr_sum + (double)samples.get(i);
		}
		curr_sum = curr_sum/((double)samples.size());
		return curr_sum;
	}
	
	public double return_variance(ArrayList<Long> samples){
		double curr_sum,variance,mean;
		curr_sum = 0.0;
		for(int i = 0; i < samples.size(); i++){
			curr_sum = curr_sum + (double)samples.get(i)*(double)samples.get(i);
		}
		curr_sum = curr_sum/((double)samples.size());
		mean = return_mean(samples);
		variance = curr_sum - mean*mean;
		return variance;
	}
	
	public int compute_heavy_traffic_approximation(){
		
		String task_Id;
		ArrayList<Long> interarrival_times = new ArrayList<Long>();
		ArrayList<Long> service_times = new ArrayList<Long>();
		ArrayList<Long> diff_list = new ArrayList<Long>();
		HashMap<String,Double> max_timeout = new HashMap<String,Double>();
		double alpha, beta;
		double contraction;
		double timeout;
		double total_timeout = 0.0;
		int computed_timeout = 0;
		String comp_id;
		String file_path = Constants.TIMEOUT_FILE_BASE_DIR + Topology_name + "/" + Topology_info + "/" + "Service_Times/";
		String inter_path = Constants.TIMEOUT_FILE_BASE_DIR + Topology_name + "/" + Topology_info + "/" + "Interarrival_Times/";
		String timeout_file_path = Constants.TIMEOUT_FILE_BASE_DIR + Topology_name + "/" + Topology_info + "/" + "timeout_append.txt";
		int i = 0;
		float avg_lambda = (float)0.0;
		int no_of_entries = 0;
		HashMap<String,Float> Comp_id_to_weight = new HashMap<String,Float>();
		
		for (String componentId : ComponentIds) {
			Comp_id_to_weight.put(componentId,(float)0.0);
		}
		
		if(task_rate_map != null){
	
			// The average lambda is estimated for each component. It is set as the component weight. Then this is used to estimate the heaviest
			//path or the most utilized path. Sum of the average lambda of each task in the component = weight of component.
			
			for (Map.Entry<String,ArrayList<ArrayList>> entry : task_rate_map.entrySet())
			{
			
				no_of_entries = 0;
				avg_lambda = (float)0.0;
				for(i=0; i<entry.getValue().size(); i++ ){
					List<Float> lambda_mu_tuple = new ArrayList();
					lambda_mu_tuple = entry.getValue().get(i);
					avg_lambda = avg_lambda + lambda_mu_tuple.get(0);
					no_of_entries++;
				}
				avg_lambda = avg_lambda/(no_of_entries);
				comp_id = Task_to_Component_map.get(Integer.parseInt(entry.getKey()));
				if(comp_id != null && !comp_id.isEmpty()){
					Comp_id_to_weight.put(comp_id,Comp_id_to_weight.get(comp_id) + avg_lambda); 
				}
			}
		
			set_node_weights(Comp_id_to_weight);
			find_heaviest_weighted_path();
			
			for (Map.Entry<String,ArrayList<ArrayList>> entry : task_rate_map.entrySet())
			{
			    task_Id = entry.getKey();
			    comp_id = Task_to_Component_map.get(Integer.parseInt(task_Id));
		    	
			    for(i = 0; i < entry.getValue().size(); i++){
			    	interarrival_times.add((Long)entry.getValue().get(i).get(3));
			    	
			    	service_times.add((Long)entry.getValue().get(i).get(2));
			    	
			    	diff_list.add((Long)(entry.getValue().get(i).get(2)) - (Long)(entry.getValue().get(i).get(3)));
			    	
			    }
			    
			    alpha = -1*(return_mean(diff_list));
			    beta = return_variance(diff_list);
			    contraction = 2*alpha/beta;
			    timeout = 2.9957/contraction;
			    
			    if(max_timeout.containsKey(comp_id)){
			    	if(max_timeout.get(comp_id) < timeout){
			    		max_timeout.put(comp_id,timeout);
			    	}
			    	
			    }
			    else{
		    		max_timeout.put(comp_id,timeout);
		    	}
			    
			    
			    
			    
			    /*	LOGGING SERVICE AND INTERARRIVAL TIMES TO SEPARATE FILES */
			    /*
			    if(Log_enabled > 0){
			    try{
		  			File statistics_file_ptr = new File(file_path + "Task_" + task_Id + "_Service_times.txt");
		  			statistics_file_ptr.getParentFile().mkdirs(); //Created the necessary folders in case the parent directories are absent
		  			PrintWriter statistics_out_ptr = new PrintWriter(new BufferedWriter(new FileWriter(statistics_file_ptr, true)));// true for append
		  			for(int i = 0; i < service_times.size(); i++){
		  				statistics_out_ptr.println(service_times.get(i)); 
		  			}
		  			 
		  			statistics_out_ptr.close();
		  		}
		  		catch (Exception e) {				        
		  		}
			    
			    try{
		  			File statistics_file_ptr = new File(inter_path + "Task_" + task_Id + "_Interarrival_times.txt");
		  			statistics_file_ptr.getParentFile().mkdirs(); //Created the necessary folders in case the parent directories are absent
		  			PrintWriter statistics_out_ptr = new PrintWriter(new BufferedWriter(new FileWriter(statistics_file_ptr, true)));// true for append
		  			for(int i = 0; i < interarrival_times.size(); i++){
		  				statistics_out_ptr.println(interarrival_times.get(i)); 
		  			}
		  			 
		  			statistics_out_ptr.close();
		  		}
		  		catch (Exception e) {				        
		  		}
			    }
			    */
			    
			    interarrival_times = new ArrayList<Long>();
			    service_times = new ArrayList<Long>();
			    diff_list = new ArrayList<Long>();
			    
			    
		 }
		
		 for (Map.Entry<String,Double> entry : max_timeout.entrySet())
		 {
			
			if(heaviest_path.contains(entry.getKey())){
				total_timeout += entry.getValue();
			}
		 }
		
		
		System.out.println("Total Heavy traffic approx timeout = " + total_timeout);
			
			
		}
		
		computed_timeout = (int)Math.ceil(total_timeout/((double) Math.pow(10,9)));
		
		
		
		/*  Logging timeout values	*/
		if(Log_enabled > 0){
		try{
  			File statistics_file_ptr = new File(timeout_file_path);
  			statistics_file_ptr.getParentFile().mkdirs(); //Created the necessary folders in case the parent directories are absent
  			PrintWriter statistics_out_ptr = new PrintWriter(new BufferedWriter(new FileWriter(statistics_file_ptr, true)));// true for append
  			
  			statistics_out_ptr.println(computed_timeout); 
  			
  			 
  			statistics_out_ptr.close();
  		}
  		catch (Exception e) {				        
  		}
		}
  		
		if (computed_timeout > 0 && computed_timeout < Constants.MAX_TIMEOUT)
			return computed_timeout;
		else 
			if(computed_timeout > Constants.MAX_TIMEOUT){
				return Constants.MAX_TIMEOUT;
			}
			else
				return 1;

		
			
	}
	
	public void MM1_approximation(){
		
		
		int task_Id;
		
		float lambda;
		float mu;
		float sojourn_percentile;
		
		
		System.out.println("Started MM1 approximation");
		
		if(task_rate_map != null){
			for (Map.Entry<String,ArrayList<ArrayList>> entry : task_rate_map.entrySet())
			{
				task_Id = Integer.parseInt(entry.getKey());
				List<Float> lambda_mu_tuple = new ArrayList();
				lambda_mu_tuple = entry.getValue().get(0);
				lambda = ((float)lambda_mu_tuple.get(0));///(float)1000000);//incoming traffic
				mu = ((float)lambda_mu_tuple.get(1));///(float)1000000);//service rate
				if(mu <= lambda){
					this.mm1_timeout =  Constants.MAX_TIMEOUT;
					System.out.println("MM1 timeout = " + mm1_timeout);
					return;
				}
				sojourn_percentile = (float) Math.log(1/(1-Constants.TIMEOUT_PERCENTILE ))/(mu - lambda);
				if(sojourn_percentile >= Constants.MAX_TIMEOUT){
					this.mm1_timeout =  Constants.MAX_TIMEOUT;
					System.out.println("MM1 timeout = " + mm1_timeout);
					return;
				}
				graph.get(Task_to_Component_map.get(task_Id)).set_weight(Math.max(sojourn_percentile, graph.get(Task_to_Component_map.get(task_Id)).weight));
	    	
			}
			
			System.out.println("Done this");
			find_heaviest_weighted_path();
			if(mm1_timeout < 1)
				this.mm1_timeout = 1;
			if(mm1_timeout > Constants.MAX_TIMEOUT)
				this.mm1_timeout = Constants.MAX_TIMEOUT;
			
		}
		else{
			this.mm1_timeout = Constants.MAX_TIMEOUT;
		}
		
		System.out.println("MM1 timeout = " + mm1_timeout);
	}
	
	
}