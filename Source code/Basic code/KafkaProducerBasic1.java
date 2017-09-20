package com;


import java.util.Properties;
import java.util.Scanner;

import org.I0Itec.zkclient.ZkClient;

import kafka.admin.AdminUtils;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;

public class KafkaProducerBasic1 {
//	static ZkClient zkClient = new ZkClient("localhost:2181", 10000, 10000,
//			ZKStringSerializer$.MODULE$);
	public String createTopic(String topicName) {
		ZkClient zkClient = new ZkClient("localhost:2181", 10000, 10000,
				ZKStringSerializer$.MODULE$);
		// ZkClient zkClient = new ZkClient("localhost:21810", 10000, 10000,
		// ZKStringSerializer$.MODULE$);
		// zkClient.deleteRecursive(ZkUtils.getTopicPath(topicName));
		Properties topicConfig = new Properties();
		AdminUtils.createTopic(zkClient, topicName, 1, 1, topicConfig);
		// zkClient.close();

		return topicName;
	}
	public static void main(String[] args) {
		Scanner s=new Scanner(System.in);
		System.out.println("enter a topic name to create::::");
		String topicName=s.nextLine();
		KafkaProducerBasic k=new KafkaProducerBasic();
		
		k.createTopic(topicName);
		Properties ppts = new Properties();
		//ppts.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		ppts.put("metadata.broker.list", "localhost:9092");
		//ppts.put("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094");
		//ppts.put("metadata.broker.list", "localhost:9092");
		ppts.put("serializer.class", "kafka.serializer.StringEncoder");
		ppts.put("batch.size",102);
		ppts.put("linger.ms",1);
		// ppts.put("log.retention.minutes", 2);
		// ppts.put("auto.offset.reset", "smallest");
//		ppts.put("message.max.bytes", "1073741824");
		// replica.fetch.max.bytes=15728640
		// ppts.put("replica.fetch.max.bytes", "1073741824");
		ProducerConfig producerConfig = new ProducerConfig(ppts);
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
//		KeyedMessage<String, String> message = new KeyedMessage<String, String>(Key, Message);
//		producer.send("hai");
		System.out.print("Enter message to send to kafka broker::: ");
String msg = null;
msg = s.nextLine(); // Read message from console
//Define topic name and message
KeyedMessage<String, String> keyedMsg = new KeyedMessage<String, String>(topicName, msg);
producer.send(keyedMsg);
		// System.out.println(message);
		producer.close();
		
	}
	


}
