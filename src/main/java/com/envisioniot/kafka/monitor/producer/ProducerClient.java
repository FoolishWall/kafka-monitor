package com.envisioniot.kafka.monitor.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.awt.event.KeyListener;
import java.util.Properties;

/**
 * @author qiang.bi
 * @date 2020/7/1 16:26
 **/
public class ProducerClient {
    Logger log = Logger.getLogger(ProducerClient.class);

    private String topicName;
    private int numPartitions;
    private int replicationFactor;
    private String broker_address;
    private  KafkaProducer<String,String> producer;

    private long startProduceTime;

    private void  initConfig(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker_address);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        this.producer = new KafkaProducer<String, String>(properties);
    }

    public ProducerClient(String topicName, String broker_address) {
        this.topicName = topicName;
        this.broker_address = broker_address;
        initConfig();
    }

    public void sendMessage(String message){
        final ProducerRecord<String,String> record = new
                ProducerRecord<String, String>(topicName,null,
                System.currentTimeMillis(),null,message);

        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (null != e){
                    log.error("send error" + e.getMessage());
                }else {
                    System.out.println(record);
                    startProduceTime = record.timestamp();
                    log.info("send successfully: record" + record);
//                    log.info(String.format("offset:%s,partition:%s",recordMetadata.offset(),recordMetadata.partition()));
                }
            }
        });
        producer.close();
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public void setBroker_address(String broker_address) {
        this.broker_address = broker_address;
    }

    public void setProducer(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public String getBroker_address() {
        return broker_address;
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }

    public long getStartProduceTime() {
        return startProduceTime;
    }
}
