package com.envisioniot.kafka.monitor.demo;

import com.envision.eos.commons.utils.LionUtil;
import com.envisioniot.kafka.monitor.constant.LionConstant;
import com.envisioniot.kafka.monitor.entity.KafkaTopic;
import com.envisioniot.kafka.monitor.service.KafkaService;
import com.envisioniot.kafka.monitor.service.impl.KafkaServiceImpl;
import org.apache.log4j.Logger;

/**
 * @author qiang.bi
 * @date 2020/7/3 18:32
 **/
public class CreateTopic {
    private static final Logger log = Logger.getLogger(CreateTopic.class);
    private static final String KAFKA_ZK_ADDRESS = LionUtil.getStringValue(LionConstant.
            KAFKA_ZK_ADDRESS_KEY);
    public static void main(String[] args) {

        String topicName = "KAFKA_MONITOR_TEST";
        int numPartitions = 8;
        int replicationFactor = 2;

        //0.创建topic
        KafkaService kafkaService = new KafkaServiceImpl();
        kafkaService.createTopic(topicName,numPartitions,replicationFactor, KAFKA_ZK_ADDRESS);
    }
}
