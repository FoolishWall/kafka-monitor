package com.envisioniot.kafka.monitor.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author qiang.bi
 * @date 2020/7/1 16:26
 **/
public class ProducerClient {
    private static Logger log = Logger.getLogger(ProducerClient.class);

    private String topicName;
    private String broker_address;
    private KafkaProducer<String,String> producer;

    private long endProduceTime;

    private void  initConfig(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker_address);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.RETRIES_CONFIG,0);
        this.producer = new KafkaProducer<String, String>(properties);
    }

    public ProducerClient(String topicName, String broker_address) {
        this.topicName = topicName;
        this.broker_address = broker_address;
        initConfig();
    }

    public void sendMessage(String message,Integer partition){
        final ProducerRecord<String,String> record = new
                ProducerRecord<String, String>(topicName,partition,
                System.currentTimeMillis(),null,message);
        log.info("start produce message time:" + record.timestamp());
        Future<RecordMetadata> future = producer.send(record);
        producer.flush();
        try {
            RecordMetadata recordMetadata = future.get();
            endProduceTime = System.currentTimeMillis();
            log.info("endProduceTime : " + endProduceTime);
        } catch (InterruptedException e) {
            log.error("send error" + e.getMessage());
        } catch (ExecutionException e) {
            log.error("send error" + e.getMessage());
        }finally {
            producer.close();
        }
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
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

    public String getBroker_address() {
        return broker_address;
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }

    public long getEndProduceTime() {
        return endProduceTime;
    }

}
