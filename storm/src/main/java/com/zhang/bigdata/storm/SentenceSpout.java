package com.zhang.bigdata.storm;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SentenceSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = -6626536502743983719L;
	private String[] sentences = null;
	private Random random = null;
	private SpoutOutputCollector collector = null;
	
	@Override
	public void nextTuple() {
		String sentence = sentences[random.nextInt(4)];
		
		collector.emit(new Values(sentence));
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		sentences = new String[] {"A apple is a iphone","B tree is big and black","C color very beauty", "D dogs are best friends with humen"};
		random = new Random();
		collector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("sentence"));
	}

}
