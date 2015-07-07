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
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

import storm.starter.bolt.Central_PT_collector_Bolt;
import storm.starter.bolt.Collect_Processing_Time_Bolt;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology_Monitored {

  public static class ExclamationBolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
    	
      Long start_time,end_time,process_time;
      start_time = System.nanoTime();
      _collector.emit("StreamOne",tuple, new Values(tuple.getString(0) + "!!!"));
      _collector.ack(tuple);
      end_time = System.nanoTime();
      process_time = end_time - start_time;
      _collector.emit("exclamation_bolt_PT_stream",new Values(process_time));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      //declarer.declare(new Fields("word"));
      declarer.declareStream("StreamOne", new Fields("word"));
  	  declarer.declareStream("exclamation_bolt_PT_stream", new Fields ("process time"));
    }


  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new TestWordSpout(), 10);
    builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word");
    builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1","StreamOne");
    Collect_Processing_Time_Bolt Monitor_Bolt1 = new Collect_Processing_Time_Bolt();
    Monitor_Bolt1.set_output_stream_name("excalmation_bolt1_PT");
    Monitor_Bolt1.set_granularity(1);
    
    Collect_Processing_Time_Bolt Monitor_Bolt2 = new Collect_Processing_Time_Bolt();
    Monitor_Bolt2.set_output_stream_name("excalmation_bolt2_PT");
    Monitor_Bolt2.set_granularity(1);
    
    Central_PT_collector_Bolt Central_Monitor_Bolt = new Central_PT_collector_Bolt();
    Central_Monitor_Bolt.set_topology_name("Exclamation_topology");
    
    builder.setBolt("Monitor-Bolt-1",Monitor_Bolt1,1).fieldsGrouping("exclaim1","exclamation_bolt_PT_stream",new Fields("process time"));
    builder.setBolt("Monitor-Bolt-2",Monitor_Bolt2,1).fieldsGrouping("exclaim2","exclamation_bolt_PT_stream",new Fields("process time"));
    builder.setBolt("Central-Monitor", Central_Monitor_Bolt,1).fieldsGrouping("Monitor-Bolt-1",Monitor_Bolt1.output_stream_name, new Fields("pdf_string")).fieldsGrouping("Monitor-Bolt-2",Monitor_Bolt2.output_stream_name, new Fields("pdf_string"));

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      
      Utils.sleep(600000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}