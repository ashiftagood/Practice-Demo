package com.zhang.bigdata.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {
	
	public static void main(String[] args) throws Exception {
		TopologyBuilder tb = new TopologyBuilder();
		
		tb.setSpout("sentenceSpout", new SentenceSpout(), 2);
		tb.setBolt("spiltBolt", new SplitBolt(), 2).shuffleGrouping("sentenceSpout");
		tb.setBolt("countBolt", new CountBolt(), 3).fieldsGrouping("spiltBolt", new Fields("word"));
		
		Config config = new Config();
		
		//集群模式
		config.setNumWorkers(2);
		StormSubmitter.submitTopologyWithProgressBar("word-count", config, tb.createTopology());
		
		//本地模式
//		config.setMaxTaskParallelism(3);
//		LocalCluster lc = new LocalCluster();
//		lc.submitTopology("word-count", config, tb.createTopology());
//		Thread.sleep(5000);
//		lc.shutdown();
		
	}
}
