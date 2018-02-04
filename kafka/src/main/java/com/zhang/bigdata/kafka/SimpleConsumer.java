package com.zhang.bigdata.kafka;

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class SimpleConsumer {
	public static void main(String[] args) {
		String topic = "test-topic";
		Properties p = new Properties();
		p.setProperty("bootstrap.servers", "vm01:9092,vm02:9092,vm03:9092");
		p.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		p.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		p.setProperty("group.id","6666");
		
		Consumer<String, String> consumer = new KafkaConsumer<>(p);
		List<String> topicList = new ArrayList<String>();
		topicList.add(topic);
		consumer.subscribe(topicList);
		List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
		List<TopicPartition> topicPartitionList = new ArrayList<>();
		
		for(PartitionInfo pi : partitionInfoList) {
			topicPartitionList.add(new TopicPartition(pi.topic(), pi.partition()));
			System.out.println("Topic:" + pi.topic() + "--> Partition:" + pi.partition());
		}
		
		consumer.seekToBeginning(topicPartitionList);
		//while(true) {
			ConsumerRecords<String, String> records = consumer.poll(5000);
			records.forEach(cr -> System.out.println("key:" + cr.key() + "--->value:" + cr.value()));
			consumer.close();
		//}
	}
}
