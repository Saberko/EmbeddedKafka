package com.chendan.kafka;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.*;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class MyProducer {
	public static void main(String[] args) throws InterruptedException {
		long events = Long.parseLong(args[0]);
		Random random = new Random();
		Properties properties = new Properties();
		properties.put("metadata.broker.list", "localhost:9092");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
//		properties.put("partitioner.class", "com.chendan.kafka.SimplePartitioner");
        properties.put("request.required.acks", "1");
        
        ProducerConfig config = new ProducerConfig(properties);
        
        Producer<String, String> producer = new Producer<String, String>(config);
        
        for (long nEvents = 0;  nEvents < events; nEvents++) {
			long runtime = new Date().getTime();
//			String ip = "192.168.2." + random.nextInt(254);
//			String msg = runtime + "----" + ip;
//			//ip is the key
//			KeyedMessage<String, String> message = new KeyedMessage<String, String>("bidding", ip, msg);
//			producer.send(message);
			String uid = String.valueOf(random.nextInt(100));
			String cost = String.valueOf(random.nextDouble() * 10);
//			KeyedMessage<String, String> message = new KeyedMessage<String, String>("bidding", "hello", uid + "," + cost);
			KeyedMessage<String, String> message = new KeyedMessage<String, String>("bidding", uid, cost);
			producer.send(message);
			if (nEvents % 10 == 0) {
				Thread.sleep(1000);
			}
		}
        producer.close();
	}
}
