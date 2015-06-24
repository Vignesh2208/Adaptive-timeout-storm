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
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.starter.bolt.IntermediateRankingsBolt_Monitored;
import storm.starter.bolt.RollingCountBolt_Monitored;
import storm.starter.bolt.TotalRankingsBolt_Monitored;
import storm.starter.util.StormRunner;
import storm.starter.bolt.Central_PT_collector_Bolt;
import storm.starter.bolt.Collect_Processing_Time_Bolt;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 */
public class RollingTopWords_Monitored {

  private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
  private static final int TOP_N = 5;

  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;

  public RollingTopWords_Monitored() throws InterruptedException {
    builder = new TopologyBuilder();
    topologyName = "slidingWindowCounts";
    
    topologyConfig = new Config();
    topologyConfig.setDebug(true);
    
    //topologyConfig = createTopologyConfiguration();
    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
    
    
   
    
    wireTopology();
  }

  //private static Config createTopologyConfiguration() {
//	return topologyConfig;
    
  //}

  private void wireTopology() throws InterruptedException {
	  	String spoutId = "wordGenerator";
	    String counterId = "counter";
	    String intermediateRankerId = "intermediateRanker";
	    String totalRankerId = "finalRanker";
	    
	    Collect_Processing_Time_Bolt Monitor_Bolt1 = new Collect_Processing_Time_Bolt();
	    Monitor_Bolt1.set_output_stream_name("rolling_count_bolt_PT");
	    Monitor_Bolt1.set_granularity(1);
	    Collect_Processing_Time_Bolt Monitor_Bolt2 = new Collect_Processing_Time_Bolt();
	    Monitor_Bolt2.set_output_stream_name("intermediate_ranker_bolt_PT");
	    Monitor_Bolt2.set_granularity(1);
	    Central_PT_collector_Bolt Central_Monitor_Bolt = new Central_PT_collector_Bolt();
	    Central_Monitor_Bolt.set_topology_name("Rolling_Top_words_topology");
	    
	    builder.setSpout(spoutId, new TestWordSpout(), 5);
	    builder.setBolt(counterId, new RollingCountBolt_Monitored(9, 3), 4).fieldsGrouping(spoutId, new Fields("word"));
	    builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt_Monitored(TOP_N), 4).fieldsGrouping(counterId, "StreamOne",new Fields(
	        "obj"));
	    builder.setBolt(totalRankerId, new TotalRankingsBolt_Monitored(TOP_N)).globalGrouping(intermediateRankerId,"StreamOne");
	    //StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
	    
	    builder.setBolt("Monitor-Bolt-1",Monitor_Bolt1,1).fieldsGrouping(counterId,"rolling_count_bolt_PT_stream",new Fields("process time"));
	    builder.setBolt("Monitor-Bolt-2",Monitor_Bolt2,1).fieldsGrouping(intermediateRankerId,"abstract_ranker_bolt_PT_stream",new Fields("process time"));
	    builder.setBolt("Central-Monitor", Central_Monitor_Bolt,1).fieldsGrouping("Monitor-Bolt-1",Monitor_Bolt1.output_stream_name, new Fields("pdf_string")).fieldsGrouping("Monitor-Bolt-2",Monitor_Bolt2.output_stream_name, new Fields("pdf_string"));
	    
	    topologyConfig.setMaxTaskParallelism(3);

    
  }

  public void run(String[] args) throws InterruptedException {
	  if (args != null && args.length > 0) {
	      topologyConfig.setNumWorkers(3);

	      try {
			StormSubmitter.submitTopologyWithProgressBar(args[0], topologyConfig, builder.createTopology());
		} catch (AlreadyAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    }
	    else {	
	  
	    LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology("rolling-word-count", topologyConfig, builder.createTopology());

	    Thread.sleep(600000);
	    cluster.shutdown();
	    }
  }

  public static void main(String[] args) throws Exception {
    new RollingTopWords_Monitored().run(args);
  }
}