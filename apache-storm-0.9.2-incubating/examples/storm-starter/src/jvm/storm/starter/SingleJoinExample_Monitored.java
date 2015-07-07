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
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.bolt.Central_PT_collector_Bolt;
import storm.starter.bolt.Collect_Processing_Time_Bolt;
import storm.starter.bolt.SingleJoinBolt_Monitored;

public class SingleJoinExample_Monitored {
  public static void main(String[] args) {
    FeederSpout genderSpout = new FeederSpout(new Fields("id", "gender"));
    FeederSpout ageSpout = new FeederSpout(new Fields("id", "age"));

    TopologyBuilder builder = new TopologyBuilder();
    
    Collect_Processing_Time_Bolt Monitor_Bolt1 = new Collect_Processing_Time_Bolt();
    Monitor_Bolt1.set_output_stream_name("single_join_bolt_PT");
    Monitor_Bolt1.set_granularity(1);
    
    Central_PT_collector_Bolt Central_Monitor_Bolt = new Central_PT_collector_Bolt();
    Central_Monitor_Bolt.set_topology_name("Single_Join_topology");
    
    
    builder.setSpout("gender", genderSpout);
    builder.setSpout("age", ageSpout);
    builder.setBolt("join", new SingleJoinBolt_Monitored(new Fields("gender", "age"))).fieldsGrouping("gender", new Fields("id"))
        .fieldsGrouping("age", new Fields("id"));

    builder.setBolt("Monitor-Bolt-1",Monitor_Bolt1,1).fieldsGrouping("join","join_bolt_PT_stream",new Fields("process time"));
    builder.setBolt("Central-Monitor", Central_Monitor_Bolt,1).fieldsGrouping("Monitor-Bolt-1",Monitor_Bolt1.output_stream_name, new Fields("pdf_string"));
    
    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
        conf.setNumWorkers(3);

        try {
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
      }
    else{
    
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("join-example", conf, builder.createTopology());

    for (int i = 0; i < 10000; i++) {
      String gender;
      if (i % 2 == 0) {
        gender = "male";
      }
      else {
        gender = "female";
      }
      genderSpout.feed(new Values(i, gender));
    }

    for (int i = 600000; i > 0; i--) {
      ageSpout.feed(new Values(i, i + 20));
    }

    Utils.sleep(600000);
    cluster.shutdown();
    
      }
  }
}