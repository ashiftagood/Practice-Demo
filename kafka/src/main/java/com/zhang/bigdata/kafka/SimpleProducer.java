package com.zhang.bigdata.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer {
	
	public static void main(String[] args) throws Exception {
		String topic = "test-topic";
		Properties p = new Properties();
		p.setProperty("bootstrap.servers", "vm01:9092,vm02:9092,vm03:9092");
		p.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		p.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String, String> producer = new KafkaProducer<>(p);
		
		int i = 0;
		while(i < 100) {
			producer.send(new ProducerRecord<String, String>(topic, "key"+i, "message" + i));
			Thread.sleep(2000);
			i++;
		}
		producer.close();
	}
}
