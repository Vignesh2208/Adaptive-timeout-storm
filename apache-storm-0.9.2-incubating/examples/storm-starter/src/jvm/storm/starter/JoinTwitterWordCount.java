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
public class JoinTwitterWordCount {
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
    String [] keywords = {"Germanwings"};
    int rate = 10;
    int sample_size = 0;
    
    int enable_adaptive_timeout = 1;
    int enable_fault_injector = 0;
    String mode = "";
    
    //String Adaptive_Timeout_mode = "END_TO_END";
    String Adaptive_Timeout_mode = "QUEUEING MODEL Heavy traffic";
    String topology_name = "Join_twitter_topology";
    int Tick_tuple_freq = 15;
    int iteration_no = 1;
    String topology_info = "default-info_" + "rate_" + rate + "_iteration_no_" + iteration_no;
    int alt_rate = 0; // 0 - disabled. Otherwise, spout alternates between rate and alt_rate every 2 minutes.
    
    if(args.length >= 3){  	  
    	
    	// specified rate is of the form <rate>_<iteration_no> - this has to be split.
        String[] parts = args[1].split("_");
        rate = Integer.parseInt(parts[0]);
        iteration_no = Integer.parseInt(parts[1]);
        
        //alt_rate = rate + 5; // Uncomment this line to make rate alternate
    	  
        builder.setSpout("spout1", new TwitterSampleSpout("TU5xcNbuL4ydKqeGTOPATIe8V","lS3EkYEGT75BWewVws9h4naqsSHWYYOcY2kDMsqnFj5fT6MwRQ","2477902411-hZPgikH7ulbVZaQo7zhk7aKqnW9QZWkr2z9uFqg","plLGbufU0DjS1NAFEXgGgfhIlRdUUQtlvw6wc4BGtCf9S",keywords,rate,alt_rate),1);
        builder.setSpout("spout2", new TwitterSampleSpout("zixFymBeTTXm37pFkdZfrv70v","XlBsIzwsShfhjoj5eSoFEAlWe8SBze0CgnkaQ6KWejewk7k5eR","2477902411-z7K4QkfklYOUYRIjtoMD4qyraIYqofGlEd7uYKh","Tyg71AAq6yG8Ucc2Y2n4sE6ClaDjt8kNyhd2mJt6qapXn",keywords,rate,alt_rate),1);
  	
  	
  		topology_info = "rate_" + rate + "_iteration_no_" + iteration_no + "_" + keywords[0];
  		
  		if(alt_rate != 0){
  			topology_info += "_alternating";
  		}  		

  		mode = args[2];
  		if(mode.equals("MM1")){
  			enable_adaptive_timeout = 1;
  			Adaptive_Timeout_mode = "QUEUEING MODEL MM1";
  			Tick_tuple_freq = 15;
  			
  		}
  		else{
  			if(mode.equals("HT")){
  				enable_adaptive_timeout = 1;
  				Adaptive_Timeout_mode = "QUEUEING MODEL Heavy Traffic";
  				Tick_tuple_freq = 15;
  			
  			}
  			else{
  				if(mode.equals("END_TO_END")){
  					enable_adaptive_timeout = 1;
  	  				Adaptive_Timeout_mode = "END_TO_END";
  	  				Tick_tuple_freq = 15;
  				}
  				else{
  					Tick_tuple_freq = Integer.parseInt(mode.replaceAll("[^0-9]", ""));
  					enable_adaptive_timeout = 0;
  	  				Adaptive_Timeout_mode = "NORMAL";
  				}
  			}
  		}
  		
  		if(args.length == 4){
  			sample_size = Integer.parseInt(args[3]);
  			enable_fault_injector = 1;
  		}
  		if(enable_adaptive_timeout == 1){
  			topology_info = topology_info + "_adaptive_timeout_enabled" + "_sample_size_" + sample_size;
  			topology_name = topology_name +  "_" + mode;
  		}
  		else{
  			topology_info = topology_info + "_adaptive_timeout_disabled" + "_sample_size_" + sample_size;
  			topology_name = topology_name + "_" + mode;
  		}
    }else{
    
    
    builder.setSpout("spout1", new TwitterSampleSpout("TU5xcNbuL4ydKqeGTOPATIe8V","lS3EkYEGT75BWewVws9h4naqsSHWYYOcY2kDMsqnFj5fT6MwRQ","2477902411-hZPgikH7ulbVZaQo7zhk7aKqnW9QZWkr2z9uFqg","plLGbufU0DjS1NAFEXgGgfhIlRdUUQtlvw6wc4BGtCf9S",keywords,rate,alt_rate), 1);
    builder.setSpout("spout2", new TwitterSampleSpout("zixFymBeTTXm37pFkdZfrv70v","XlBsIzwsShfhjoj5eSoFEAlWe8SBze0CgnkaQ6KWejewk7k5eR","2477902411-z7K4QkfklYOUYRIjtoMD4qyraIYqofGlEd7uYKh","Tyg71AAq6yG8Ucc2Y2n4sE6ClaDjt8kNyhd2mJt6qapXn",keywords,rate,alt_rate), 1);
    }

    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout1").shuffleGrouping("spout2");
    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

    Config conf = new Config();
    
    conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,Tick_tuple_freq);
    conf.setDebugPrintEnabled(false);
    conf.setDebugLogEnabled(true);
        

    if(enable_adaptive_timeout == 1){
    	
    	conf.EnableAdaptiveTimeout();
    	conf.SetTimeoutMode(Adaptive_Timeout_mode);
    }
    else{
    	conf.SetTimeoutMode("NORMAL");
    }
    if(enable_fault_injector == 1){
    	conf.EnableFaultInjector(sample_size);
    }
    
    conf.SetTopologySpecificInfo(topology_name,topology_info);

    
    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);
      
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      //conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("join-twitter-count", conf, builder.createTopology());

      Thread.sleep(60000);

      cluster.shutdown();
    }
  }
}