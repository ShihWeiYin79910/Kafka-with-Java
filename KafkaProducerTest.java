package com.ey.data.streaming.kafka;

import java.sql.ResultSet;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.ey.database.connector.DBConnector;

public class KafkaProducerTest implements Runnable {

	private final KafkaProducer<String, String> producer;
    private final String topic;
    public KafkaProducerTest(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        this.producer = new KafkaProducer<String, String>(props);
        this.topic = topicName;
    }

    public void run() {
        try {
        	//Read Database Data
        	DBConnector DC = new DBConnector("host","port","dbname","username","password");
        	DC.conn.setAutoCommit(false);
        	ResultSet tResult = DC.executeQuery("Select * from TEST_Source");
        	
        	String tMessageKey = "TEST_Source";
        	String tMessageValue = null;
        	
        	while(tResult.next()) {
            	tMessageValue = tResult.getString("EMPID") + "$@$" + tResult.getString("EMPNAME") + "$@$" + tResult.getString("EMPAGE") + "$@$" + tResult.getString("FLAG");
            	System.out.println(tMessageValue);
            	producer.send(new ProducerRecord<String, String>(topic, tMessageKey, tMessageValue));
            	
        	}
        	
        	//Close Connection
        	DC.DBClose();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public static void main(String args[]) {
        KafkaProducerTest test = new KafkaProducerTest("test");
        Thread thread = new Thread(test);
        thread.start();
    }

}
