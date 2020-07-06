package com.envisioniot.kafka.monitor.constant;

/**
 * @author qiang.bi
 * @date 2020/7/3 18:06
 **/
public enum CreateTopicStatus {

    SUCCESS(0,"SUCCESS"),
    EXISTS(1,"EXISTS"),
    FAIL(2,"FAIL");

    private int code;
    private String status;


    CreateTopicStatus(int code, String status) {
        this.code = code;
        this.status = status;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
