package com.data.streaming.kafka;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.ey.database.connector.DBConnector;

public class KafkaConsumerTest implements Runnable {
	private final KafkaConsumer<String, String> consumer;
	private ConsumerRecords<String, String> msgList;
	private final String topic;
	private static final String GROUPID = "groupA";

	public KafkaConsumerTest(String topicName) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", GROUPID);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		this.consumer = new KafkaConsumer<String, String>(props);
		this.topic = topicName;
		this.consumer.subscribe(Arrays.asList(topic));
	}

	public void run() {
		try {
			
			//Create Database Connection
        	DBConnector DC = new DBConnector("host","port","dbname","username","password");
        	DC.conn.setAutoCommit(false);
        	int ii =1;
        	
			while (true) {
				Boolean tFlg = false;
				 
				msgList = consumer.poll(1000);
				for (ConsumerRecord<String, String> record : msgList) {
					System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(),	record.value());
					
					//Parse Value Column
					String[] tStringArrary = record.value().split("\\$@\\$");
					
					//Data Transformation
					int tBornYear = Integer.parseInt(tStringArrary[tStringArrary.length-2]) + 1911;
					String tFlag = tStringArrary[tStringArrary.length-1].trim();
					if (tFlag.toLowerCase() == "yes" || tFlag.toLowerCase() == "ok") {
						tFlag = "T";
					} else 
						tFlag = "F";
					
					//Insert to Target Table
					String tQuery = "INSERT INTO TEST_Target (\"EMPID\",\"EMPNAME\",\"EMPAGE\",\"FLAG\") " + " VALUES (" +"'"+tStringArrary[tStringArrary.length-4]+"','"+tStringArrary[tStringArrary.length-3]+"','"+tBornYear+"','"+tFlag+ "')";
					System.out.println(tQuery);
					
					Boolean i = DC.executeTrUpdate(tQuery);
					String tErrorMsg1 = "資料在數據表中不存在，更新失敗";
					
					if (!i) {
						DC.DBClose();
						throw new Exception(tErrorMsg1);
					}
					tFlg = true;
				}
				
				System.out.println("Not in for-loop" + tFlg.toString());
				if (ii>2 & tFlg==false) {
					DC.DBClose();
					consumer.close();
					break;
				}
				ii++;
				TimeUnit.SECONDS.sleep(1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}

	}

	public static void main(String args[]) {
		KafkaConsumerTest test1 = new KafkaConsumerTest("test");
		Thread thread1 = new Thread(test1);
		thread1.start();
	}
}
