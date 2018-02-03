package com.zhang.bigdata.storm;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class CountBolt extends BaseRichBolt {

	private static final long serialVersionUID = 8276744477192578640L;
	private Map<String, Integer> wordCount;
	
	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		int num = tuple.getIntegerByField("num");
		if(wordCount.containsKey(word)) {
			wordCount.put(word, wordCount.get(word) + num);
		} else {
			wordCount.put(word, num);
		}
		System.out.println("word: " + word + " ---> count: " + wordCount.get(word));
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		wordCount = new HashMap<String, Integer>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		
	}

}
