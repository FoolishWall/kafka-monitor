package com.envisioniot.kafka.monitor.service;

import com.envisioniot.kafka.monitor.entity.KafkaTopic;

/**
 * @author qiang.bi
 * @date 2020/7/3 17:11
 **/
public interface KafkaService {

    KafkaTopic createTopic(String topicName, int numPartitions, int replicationFactor, String zkUrl);

    boolean deleteTopic(String topicName);
}
