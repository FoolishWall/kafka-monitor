package com.envisioniot.kafka.monitor.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Properties;

/**
 * @author qiang.bi
 * @date 2020/7/1 16:26
 **/
public class ConsumerClient {
    Logger log = Logger.getLogger(ConsumerClient.class);

    private long endProduceTime;
    private long startConsumeTime;
    private long endConsumeTime = 0;

    private String topicName;
    private String broker_address;
    private String consumeGroup;
    private KafkaConsumer<String,String> consumer;

    public ConsumerClient(String topicName,String broker_address,String consumeGroup) {
        this.topicName = topicName;
        this.broker_address = broker_address;
        this.consumeGroup = consumeGroup;
        initConfig();
    }

    private void initConfig(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers",broker_address);
        properties.put("group.id",consumeGroup);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("enable.auto.commit", "false");
        this.consumer = new KafkaConsumer<String, String>(properties);
    }

    //动态分区
    public void subscribeMessage(){
        consumer.subscribe(Collections.singletonList(topicName));
        //handle message
        timecRecord();
    }

    /**
     * 订阅指定的分区
     * @param partition 分区编号
     */
    public void assignMessage(String topic,int partition){
        consumer.assign(Collections.singletonList(new TopicPartition(topic, partition)));
        //handle message
        timecRecord();
    }

    public void timecRecord(){
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10);
            startConsumeTime = System.currentTimeMillis();
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
                endProduceTime = record.timestamp();
                consumer.commitAsync();
                endConsumeTime = System.currentTimeMillis();
                log.info("consumer successfully. record: "+record);
            }
            if (endConsumeTime!= 0){
                consumer.close();
                break;
            }
        }
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public void setBroker_address(String broker_address) {
        this.broker_address = broker_address;
    }

    public void setConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    public void setConsumeGroup(String consumeGroup) {
        this.consumeGroup = consumeGroup;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getBroker_address() {
        return broker_address;
    }

    public KafkaConsumer<String, String> getConsumer() {
        return consumer;
    }

    public long getEndProduceTime() {
        return endProduceTime;
    }

    public long getStartConsumeTime() {
        return startConsumeTime;
    }

    public long getEndConsumeTime() {
        return endConsumeTime;
    }

    public String getConsumeGroup() {
        return consumeGroup;
    }
}
