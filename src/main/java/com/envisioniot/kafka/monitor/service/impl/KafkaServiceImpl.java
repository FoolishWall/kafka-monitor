package com.envisioniot.kafka.monitor.service.impl;

import com.envisioniot.kafka.monitor.constant.CreateTopicStatus;
import com.envisioniot.kafka.monitor.entity.KafkaTopic;
import com.envisioniot.kafka.monitor.service.KafkaService;
import com.envisioniot.kafka.monitor.util.ZookeeperUtils;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * @author qiang.bi
 * @date 2020/7/3 17:12
 **/
public class KafkaServiceImpl implements KafkaService {
    private static final Logger log = Logger.getLogger(KafkaServiceImpl.class);

    public KafkaTopic createTopic(String topicName, int numPartitions
            , int replicationFactor, String zkUrl) {
        ZkUtils zkUtils = null;
        try {
            zkUtils = ZookeeperUtils.getZkUtilsByZkUrl(zkUrl);
            //判断topic是否存在
            if (AdminUtils.topicExists(zkUtils,topicName)){
                log.warn(String.format("topic [%s] already exists.", topicName));
                return new KafkaTopic(CreateTopicStatus.EXISTS.getStatus());
            }
            AdminUtils.createTopic(zkUtils,topicName,numPartitions,replicationFactor,
                    new Properties(), new RackAwareMode.Enforced$());
        } catch (Exception e) {
            log.error("create topic error " + e.getMessage());
            return new KafkaTopic(CreateTopicStatus.FAIL.getStatus());
        } finally {
            if (zkUtils != null){
                zkUtils.close();
            }
        }
        log.info("create topic successfully !");
        return new KafkaTopic(CreateTopicStatus.SUCCESS.getStatus());
    }


    public boolean deleteTopic(String topicName) {
        return false;
    }
}
