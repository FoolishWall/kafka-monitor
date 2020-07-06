package com.envisioniot.kafka.monitor.entity;

/**
 * @author qiang.bi
 * @date 2020/7/3 18:15
 **/
public class KafkaTopic {

    private String status;

    public KafkaTopic(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "KafkaTopic{" +
                "status='" + status + '\'' +
                '}';
    }
}
