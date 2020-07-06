package com.envisioniot.kafka.monitor.demo;

import com.envisioniot.kafka.monitor.consumer.ConsumerClient;
import com.envisioniot.kafka.monitor.producer.ProducerClient;
import com.envisioniot.kafka.monitor.util.ThreadPoolUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author qiang.bi
 * @date 2020/7/1 17:57
 **/
public class KafkaMonitorDemo {

    private static final Logger log = Logger.getLogger(KafkaMonitorDemo.class);
    //private static final String broker_address = LionUtil.getStringValue(LionConstant.KAFKA_BROKER_ADDRESS_KEY);

    private static long endProduceTime;
    private static long endConsumeTime;

    private static final CountDownLatch consumerFirst = new CountDownLatch(1);

    public static void main(String[] args) {

        final String topicName = "KAFKA_MONITOR_TEST";
        String consumeGroup = "0";
        String broker_address = "kafka9001.eniot.io:9092,kafka9002.eniot.io:9092";
        //String broker_address = "127.0.0.1:9092";
        final int partition = 0 ;

        //1.创建consumer端
        final ConsumerClient consumerClient = new ConsumerClient(topicName,broker_address,consumeGroup);
        final KafkaConsumer<String,String> consumer = consumerClient.getConsumer();

        //2.启动生产端
        final ProducerClient producerClient = new ProducerClient(topicName,broker_address);

        ThreadPoolUtils.getSingleThreadPool("producer").execute(new Runnable() {
            public void run() {
                try {
                    consumerFirst.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.info("后启动producer");
                producerClient.sendMessage("kafka monitor : 测试消息",partition);
            }
        });

        //3.进行统计
        ThreadPoolUtils.getSingleThreadPool("counter").execute(new Runnable() {

            public void run() {
                int index = 1;
                while (true){
                    //动态分区
                    //consumerClient.subscribeMessage();
                    //订阅指定分区的数据
                    consumerClient.assignMessage(partition);
                    ConsumerRecords<String, String> records = null;
                    try {
                        records = consumer.poll(Long.MAX_VALUE);
                        if (index == 1){
                            consumerFirst.countDown();
                            log.info("先启动consumer");
                            index -- ;
                        }
                    } catch (Exception e) {
                        log.error("consumer poll error" + e.getMessage());
                    }
                    if (records != null) {
                        for (ConsumerRecord<String, String> record : records) {
                            List<ConsumerRecord<String, String>> partitionRecords = records.records(new TopicPartition(topicName,partition));
                            //获取最新消息offset
                            long offset = partitionRecords.get(partitionRecords.size() -1 ).offset();
                            //消费最新消息
                            if (record.offset() == offset){
                                log.info("start consume time: " + record.timestamp());
                                endConsumeTime = System.currentTimeMillis();
                                endProduceTime = producerClient.getEndProduceTime();
                                if (endProduceTime != 0){
                                    log.info("consume successfully. The time interval is "+ (endConsumeTime-endProduceTime)+"ms");
                                }else {
                                    log.info("The consumption time is earlier than the production time !");
                                }
                            }
                        }
                    }
                }
            }
        });
    }
}


