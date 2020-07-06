package com.envisioniot.kafka.monitor.consumer;

import com.envisioniot.kafka.monitor.entity.KafkaTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author qiang.bi
 * @date 2020/7/1 16:26
 **/
public class ConsumerClient {
    private static Logger log = Logger.getLogger(ConsumerClient.class);

    private long startConsumeTime;
    private long endConsumeTime;

    private String topicName;
    private String broker_address;
    private String consumeGroup;
    private int partition;
    private KafkaConsumer<String,String> consumer;

    public ConsumerClient(String topicName,String broker_address
            ,String consumeGroup) {
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
        properties.put("auto.offset.reset","earliest");
        this.consumer = new KafkaConsumer<String, String>(properties);
    }

    //动态分区
    public void subscribeMessage(){
        consumer.subscribe(Collections.singletonList(topicName));
    }

    /**
     * 订阅指定的分区
     * @param partition 分区编号
     */
    public void assignMessage(int partition){
        consumer.assign(Collections.singletonList(new TopicPartition(getTopicName(), partition)));
    }

    public void consumeLatestMessage(ConsumerRecords<String, String> records){
        List<ConsumerRecord<String, String>> partitionRecords = records.records(new TopicPartition(topicName,partition));
        //获取最新消息offset
        long offset = partitionRecords.get(partitionRecords.size() -1 ).offset();
        for (ConsumerRecord<String, String> record : records) {
            if (record.offset() == offset){
                //消费最新消息
                System.out.println(record);
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
