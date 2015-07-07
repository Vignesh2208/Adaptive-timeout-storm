package storm.starter.bolt;
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
public class Collect_Processing_Time_Bolt extends BaseBasicBolt {
    Map<Long, Integer> counts = new HashMap<Long, Integer>();
    Map<Long,Double> pdf = new HashMap<Long,Double>(); 
    Long total_number_of_tuples = (long) 0;
    Integer granularity = 1;
    public String output_stream_name;
    Integer output_frequency = 20;
    
    public void Collect_Processing_Time_Bolt(){
    	this.granularity = 1;
    	this.output_stream_name = "Process_time_stream";
    	this.output_frequency = 20;
    	this.total_number_of_tuples = (long) 0;
    	
    }
    
    public void set_granularity(Integer granularity){
    	this.granularity = granularity;
    	
    }
    
    public void set_output_stream_name(String stream_ID){
    	this.output_stream_name = stream_ID;
    }
    
    public void set_output_tuple_frequency(Integer out_freq){
    	this.output_frequency = out_freq;
    }
    
    public static String print_pdf_to_string(Map mp) {
    	String return_value = "";
        Iterator it = mp.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            return_value = return_value + pair.getKey() +"=" + pair.getValue() + " ";
           
        }
        return return_value;
    }
    
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      
      Long bin_no;
      Double temp;
      total_number_of_tuples ++;
      
      bin_no = tuple.getLong(0)/granularity;
      //bin_no = total_number_of_tuples;
      Integer count = counts.get(bin_no);
      if(count == null){
    	  count = 1;    	  
      }
      else{
    	  count ++;    	  
      }
      counts.put(bin_no, count);
      Iterator it = counts.entrySet().iterator();
      while (it.hasNext()) {
          Map.Entry<Long,Integer> pair = (Map.Entry)it.next();
          temp = (double) ((float)pair.getValue()/(float)total_number_of_tuples);
          pdf.put(pair.getKey(), temp);
         
      }
     
      
      
   System.out.println("Output frequency = " + output_frequency);
    String batch_number,pdf_string;
    if(total_number_of_tuples % output_frequency == 0){
    	batch_number = String.valueOf(total_number_of_tuples /output_frequency);
    	System.out.println("Pdf upto batch number : " + batch_number);
		pdf_string = print_pdf_to_string(pdf);
		System.out.println(pdf_string);
		collector.emit(output_stream_name,new Values(pdf_string));
	
    }
      
    }
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream(this.output_stream_name, new Fields("pdf_string"));
	}

    
  }
