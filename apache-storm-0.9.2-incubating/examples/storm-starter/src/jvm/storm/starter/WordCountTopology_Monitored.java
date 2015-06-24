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
import storm.starter.spout.TwitterSampleSpout;
import storm.starter.bolt.Collect_Processing_Time_Bolt;
import storm.starter.bolt.Central_PT_collector_Bolt;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology_Monitored {
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
    Map<Integer, Integer> processing_times = new HashMap<Integer,Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      Long start_time, end_time,process_time;
      
      start_time = System.nanoTime();
      
      //Logic
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      collector.emit("StreamOne",new Values(word, count));
      //End-of-logic
      
      
      end_time = System.nanoTime();
      process_time = (end_time -start_time);
      System.out.println("Process_time = " + process_time);
      collector.emit("count_bolt_PT_stream",new Values(process_time));
      
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	// This stuff also has to be changed
    	declarer.declareStream("StreamOne", new Fields("word", "count"));
    	declarer.declareStream("count_bolt_PT_stream", new Fields("process time"));
      
    }
  }
  
  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();
    Collect_Processing_Time_Bolt Monitor_Bolt1 = new Collect_Processing_Time_Bolt();
    Monitor_Bolt1.set_output_stream_name("Word_count_bolt_PT");
    Monitor_Bolt1.set_granularity(1);
    Central_PT_collector_Bolt Central_Monitor_Bolt = new Central_PT_collector_Bolt();
    Central_Monitor_Bolt.set_topology_name("Word_count_topology");
    
    
    
    //builder.setSpout("spout", new RandomSentenceSpout(), 5);
    String [] keywords = {"Germanwings"};
    int max_rate = 101;
    int min_rate = 100;
    //String [] keywords = {""};

    //builder.setSpout("spout", new RandomSentenceSpout_latency(), 5);
    builder.setSpout("spout", new TwitterSampleSpout("TU5xcNbuL4ydKqeGTOPATIe8V","lS3EkYEGT75BWewVws9h4naqsSHWYYOcY2kDMsqnFj5fT6MwRQ","2477902411-hZPgikH7ulbVZaQo7zhk7aKqnW9QZWkr2z9uFqg","plLGbufU0DjS1NAFEXgGgfhIlRdUUQtlvw6wc4BGtCf9S",keywords,max_rate,min_rate), 5);

    
    
    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));
    builder.setBolt("Monitor-Bolt-1",Monitor_Bolt1,1).fieldsGrouping("count","count_bolt_PT_stream",new Fields("process time"));
    builder.setBolt("Central-Monitor", Central_Monitor_Bolt,1).fieldsGrouping("Monitor-Bolt-1",Monitor_Bolt1.output_stream_name, new Fields("pdf_string"));
    
    Config conf = new Config();
    conf.setDebug(true);


    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      //conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(600000);

      cluster.shutdown();
    }
  }
}