package com.envisioniot.kafka.monitor.demo;

import com.envisioniot.kafka.monitor.consumer.ConsumerClient;
import com.envisioniot.kafka.monitor.producer.ProducerClient;

import java.util.concurrent.CountDownLatch;

/**
 * @author qiang.bi
 * @date 2020/7/1 17:57
 **/
public class KafkaMonitorDemo {
    private static long startProduceTime;
    private static long endProduceTime;
    private static long startConsumeTime;
    private static long endConsumeTime;

    private static CountDownLatch countDownLatch = new CountDownLatch(2);
    public static void main(String[] args) throws InterruptedException {
        new Thread(new ProducerThread()).start();
        new Thread(new ConsumerThread()).start();
        countDownLatch.await();
        System.out.println("生产消费时间间隔"+(endConsumeTime-startProduceTime)+"毫秒");
    }

    static class ProducerThread implements Runnable{
        public void run() {
            String topicName = "test";
            String broker_address = "127.0.0.1:9092";
            ProducerClient producerClient = new ProducerClient(topicName,broker_address);
            producerClient.sendMessage("测试消息");
            startProduceTime = producerClient.getStartProduceTime();
            countDownLatch.countDown();
        }
    }

    static class ConsumerThread implements Runnable{
        public void run() {
            String topicName = "test";
            String broker_address = "127.0.0.1:9092";
            String consumeGroup = "0";
            ConsumerClient consumerClient = new ConsumerClient(topicName,broker_address,consumeGroup);
            consumerClient.subscribeMessage();
            endProduceTime = consumerClient.getEndProduceTime();
            startConsumeTime = consumerClient.getStartConsumeTime();
            endConsumeTime = consumerClient.getEndConsumeTime();
            countDownLatch.countDown();
        }
    }
}


