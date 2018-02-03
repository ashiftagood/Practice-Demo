package com.zhang.bigdata.storm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitBolt extends BaseRichBolt {

	private static final long serialVersionUID = 2450677948994944162L;
	private OutputCollector collector;
	
	@Override
	public void execute(Tuple tuple) {
		String sentence = tuple.getString(0);
		String[] words = sentence.split(" ");
		for(String word : words) {
			collector.emit(new Values(word, 1));
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		collector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("word","num"));
	}

}
