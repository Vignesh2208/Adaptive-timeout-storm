package storm.starter.bolt;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Central_PT_collector_Bolt extends BaseBasicBolt {
    
    Map<String,String> Stream_to_pdf_Mapper = new HashMap<String,String>();  
    String topology_name;
    String Log_directory;
    
    public void set_topology_name(String topology_name){
    	this.topology_name = topology_name;
    	//this.Log_directory = System.getProperty("user.home") + "/Storm-logs/" + this.topology_name + "_logs";
    	this.Log_directory = "/app/home/" + this.topology_name + "_logs";
    }
    
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      PrintWriter writer;
      String tuple_stream_name = tuple.getSourceStreamId();
      System.out.println("Tuple stream name = " + tuple_stream_name);
      String curr_pdf = Stream_to_pdf_Mapper.get(tuple_stream_name);
      if(curr_pdf == null){
    	  curr_pdf =  tuple.getString(0);
    	  Stream_to_pdf_Mapper.put(tuple_stream_name,curr_pdf);
    	  
      }
      else{
    	  curr_pdf =  tuple.getString(0);
    	  Stream_to_pdf_Mapper.put(tuple_stream_name,curr_pdf);
      }
      try {
    	  File rootDir = new File(System.getProperty("user.home") + "/Storm-logs");
    	  File theDir = new File(this.Log_directory);

    	  // if the directory does not exist, create it
    	  if (!rootDir.exists()) {
    	    System.out.println("Creating directory: " + this.Log_directory);
    	    boolean result = false;

    	    try{
    	        rootDir.mkdir();
    	        theDir.mkdir();
    	        result = true;
    	     } catch(SecurityException se){
    	        //handle it
    	     }        
    	     if(result) {    
    	       System.out.println("DIR created");  
    	     }
    	  }
    	  else if (!theDir.exists()){
    		  
    		  System.out.println("Creating directory: " + this.Log_directory);
      	    	boolean result = false;

      	    	try{
      	    		theDir.mkdir();
      	            result = true;
      	     } catch(SecurityException se){
      	        //handle it
      	     }        
      	     if(result) {    
      	       System.out.println("DIR created");  
      	     }
    	  }
		writer = new PrintWriter(this.Log_directory +"/" + tuple_stream_name + "_log.txt", "UTF-8");
		writer.println(curr_pdf);
	    writer.println("\n");
	    
	    writer.close();
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (UnsupportedEncodingException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
      
    }
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

    
  }


