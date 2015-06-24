package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

//Additional imports
import backtype.storm.Constants;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.lang.System;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class RandomSentenceSpout_latency  extends BaseRichSpout{
	  SpoutOutputCollector _collector;
	  Random _rand;
	  String _latencyPath;

	  Map<String, Long> _startTime = new HashMap<String, Long>(); //Stores the emit time of tuple
	  Map<String, String> _value = new HashMap<String, String>(); //Stores the value of tuple
	  
	  float _currentTimeOut = (float) 30.0;	//Initial value of the time out is 30s	
	  //float _currentMeanLatency; //Value of the mean latency
	  //double _Alpha = Constants.ADAPT_PARAMETER; //Parameter for adaptive mean algorithm
	  
	  @Override
	  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	    _collector = collector;
	    _rand = new Random();
	    //_latencyPath = Constants.TIMEOUT_FILE_BASE_DIR + "/" + context.getThisComponentId() + "_latency_" + System.nanoTime() +".txt";
        _latencyPath = "/app/home/"+ context.getThisComponentId() + "_latency_" +System.nanoTime() +".txt";
//	    try {
//	        File file = new File(_latencyPath);
//	        file.getParentFile().mkdirs(); //Created the necessary folders in case the parent directories are absent
//	    	PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file)));
//	        out.println(_currentTimeOut);
//	        out.close();
//	    } catch (IOException e) {	
//	        //exception handling left as an exercise for the reader
//	    	//e.printStackTrace();
//	    }
	    
	    //Assuming the distribution is exponential
	    //_currentMeanLatency = (float) (-Math.log(0.95)/_currentTimeOut);
	  }

	  @Override
	  public void nextTuple() {
	    Utils.sleep(100);
	    String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
	        "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
	    String sentence = sentences[_rand.nextInt(sentences.length)];
	    
	   	Long time_stamp = System.nanoTime();
	    Long hash = (long) time_stamp.hashCode();
	   	_startTime.put(hash.toString() , time_stamp);
	   	_value.put(hash.toString() , sentence);
	    _collector.emit(new Values(sentence), hash.toString());
	  }

	  @Override
	  public void ack(Object id) {
//		  if(id.equals("timeout compute")){
//		  if(true){
			  //Assuming the distribution is exponential
//			  _currentTimeOut = (float) (-Math.log(0.95)/_currentMeanLatency);
//			try {
//				PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(_timeOutPath)));
//			    out.println(_currentTimeOut);
//		        out.close();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//	        return;
//		  }
		  
		  Long value = _startTime.get(id);
		  Long stopTime = System.nanoTime();
		  if(value != null){
//			   _currentMeanLatency = (float) (_currentMeanLatency*_Alpha + value/Math.pow(10,9)*(1-_Alpha));
			   _startTime.remove(id);
//			   if(_value.get(id) != null)
//				   _value.remove(id);
			    try {
			        File file = new File(_latencyPath);
			        file.getParentFile().mkdirs(); //Created the necessary folders in case the parent directories are absent
			    	PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));//append = true
			        out.println((stopTime - value)/Math.pow(10, 6));//Printing the timouts in nano seconds
			        out.close();
			    } catch (IOException e) {
			        //exception handling left as an exercise for the reader
			    	//e.printStackTrace();
			    }

		  }
	  }

	  @Override
	  public void fail(Object id) {
		  Long value = _startTime.get(id);
		  String sentence = _value.get(id);
		  if(value != null){
			  if(sentence != null){
				  Long time_stamp = System.nanoTime();
				  Long hash = (long) time_stamp.hashCode();
				  _startTime.put(hash.toString(), time_stamp);
				  _value.put(hash.toString(), sentence);
				  _collector.emit(new Values(sentence), hash.toString());
				  _value.remove(id);
			  }	
			  _startTime.remove(id);
		  }
	  }

	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("word"));
	  }

}