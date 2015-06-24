package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.spout.RandomSentenceSpout;
import storm.starter.spout.RandomSentenceSpout_latency;
import storm.starter.spout.TwitterSampleSpout;

import java.util.HashMap;
import java.util.Map;



/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {
  public static class SplitSentence extends ShellBolt implements IRichBolt {

    public SplitSentence() {
      super("python", "splitsentence.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  public static class WordCount extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    
      
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      collector.emit(new Values(word, count));
      
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }
  
  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();
    String [] keywords = {"Charleston"};
    int rate = 10;
    int sample_size = 0;
    String topology_info = "default-info";
    int enable_adaptive_timeout = 1;
    int enable_fault_injector = 0;
    String Adaptive_Timeout_mode = "QUEUEING MODEL Heavy Traffic";
    String topology_name = "Word count topology ";
    
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
    
       
    if(args.length >= 3){  	  
  	  
  	builder.setSpout("spout", new TwitterSampleSpout("TU5xcNbuL4ydKqeGTOPATIe8V","lS3EkYEGT75BWewVws9h4naqsSHWYYOcY2kDMsqnFj5fT6MwRQ","2477902411-hZPgikH7ulbVZaQo7zhk7aKqnW9QZWkr2z9uFqg","plLGbufU0DjS1NAFEXgGgfhIlRdUUQtlvw6wc4BGtCf9S",keywords,Integer.parseInt(args[1]),0),1);
  		
  	
  	
  		topology_info = "rate_" + Integer.parseInt(args[1]) + "_" + keywords[0];
  		enable_adaptive_timeout = Integer.parseInt(args[2]);
  		
  		if(args.length == 4){
  			sample_size = Integer.parseInt(args[3]);
  			enable_fault_injector = 1;
  		}
  		if(enable_adaptive_timeout == 1){
  			topology_info = topology_info + "_adaptive_timeout_enabled" + "_sample_size_" + sample_size;
  			topology_name = topology_name + " " + Adaptive_Timeout_mode;
  		}
  		else{
  			topology_info = topology_info + "_adaptive_timeout_disabled" + "_sample_size_" + sample_size;
  			topology_name = topology_name + " NORMAL";
  		}
    }else{
    
    
    builder.setSpout("spout", new TwitterSampleSpout("TU5xcNbuL4ydKqeGTOPATIe8V","lS3EkYEGT75BWewVws9h4naqsSHWYYOcY2kDMsqnFj5fT6MwRQ","2477902411-hZPgikH7ulbVZaQo7zhk7aKqnW9QZWkr2z9uFqg","plLGbufU0DjS1NAFEXgGgfhIlRdUUQtlvw6wc4BGtCf9S",keywords,rate,0), 1);
    }

    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

    Config conf = new Config();
    
    conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 15); // This sets the tick tuple frequency to 15 secs.    
    conf.setDebugPrintEnabled(false);
    conf.setDebugLogEnabled(true);
    
    if(enable_adaptive_timeout == 1){
    	
    	conf.EnableAdaptiveTimeout();
    	conf.SetAdaptiveTimeoutMode(Adaptive_Timeout_mode);
    }
    else{
    	conf.EnableAdaptiveTimeout();
    	conf.SetAdaptiveTimeoutMode("NORMAL");
    }
    if(enable_fault_injector == 1){
    	conf.EnableFaultInjector(sample_size);
    }
    conf.SetBaseDirName(topology_name);
    conf.SetTopologySpecificInfo(topology_info);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);
      
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      //conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());    

      Thread.sleep(60000);
      cluster.shutdown();
    }
  }
}