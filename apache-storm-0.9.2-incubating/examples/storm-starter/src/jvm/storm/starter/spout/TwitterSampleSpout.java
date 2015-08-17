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

package storm.starter.spout;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.lang.Process;

@SuppressWarnings("serial")
public class TwitterSampleSpout extends BaseRichSpout {

	SpoutOutputCollector _collector;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream twitterStream;
	String consumerKey;
	String consumerSecret;
	String accessToken;
	String accessTokenSecret;
	String[] keyWords;
	String prev_text = "default";
	
	
	
   Map<String, String> _value = new HashMap<String, String>(); //Stores the value of tuple for later re-transmission
   
   int rate  = 10;
   int alt_rate = 0;
   int orig_rate = 0;
   long start_window_time = 0;
   long end_window_time = 0;
   long window_size = 120; // alternating time period betn rate and alt_rate. currently set to 120 sec - 2 mins.
   
   
   public TwitterSampleSpout(String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret, String[] keyWords,int rate, int alt_rate) {
		
		
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
		this.keyWords = keyWords;
		this.rate = rate;
		this.orig_rate = rate;
		this.alt_rate = alt_rate;
		
	}

	public TwitterSampleSpout() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		String new_keyword = new String(keyWords[0]);
		new_keyword = new_keyword.replaceAll("\\s+","");
		queue = new LinkedBlockingQueue<Status>(1000);
		_collector = collector;
		
		
		
		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
			
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onException(Exception ex) {
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}

		};

		twitterStream = new TwitterStreamFactory(
				new ConfigurationBuilder().setJSONStoreEnabled(true).build())
				.getInstance();

		twitterStream.addListener(listener);
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		AccessToken token = new AccessToken(accessToken, accessTokenSecret);
		twitterStream.setOAuthAccessToken(token);
		
		if (keyWords.length == 0) {

			twitterStream.sample();
		}

		else {

			FilterQuery query = new FilterQuery().track(keyWords);
			twitterStream.filter(query);
		}

	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		int no_emitted = 0;
		
		
		
		if(this.alt_rate != 0){
			
			if(start_window_time == 0){
				start_window_time = System.currentTimeMillis()/1000;
			}
			
			end_window_time =  System.currentTimeMillis()/1000;
			if(end_window_time - start_window_time > window_size){ // alternate between orig_rate and alt_rate after every window_size seconds
				orig_rate = rate;
				rate = alt_rate;
				alt_rate = orig_rate;
				start_window_time = end_window_time;
				
			}
			
		}
					
		while (no_emitted <= rate){
			
			
			Long time_stamp = System.nanoTime();
		    Long hash = (long) time_stamp.hashCode();
			    
		   	if(ret == null){
		   		_value.put(hash.toString() ,prev_text);
		   		_collector.emit(new Values(prev_text), hash.toString());
		   	}
		   	else{
		   		prev_text = ret.getText();
			   	_value.put(hash.toString() ,ret.getText());
			    _collector.emit(new Values(ret.getText()), hash.toString());
		   	}
					    
		    no_emitted = no_emitted + 1;
			    
		}
	 
			        
		Utils.sleep(50);
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	  @Override
	  public void ack(Object id) {
		  if(_value.get(id)!= null){
			 
			  _value.remove(id);
		  }
		
		  
	  }

	  @Override
	  public void fail(Object id) {
		  
		  if(_value.get(id) != null){
			  
			  //System.out.println("Reemitted tuple " + _value.get(id));
			  _collector.emit(new Values(_value.get(id)), id); // emit back with the same id
			  
		  }
		  
		  	  
	  }
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("word"));
	}

}