package com.envisioniot.kafka.monitor.util;

import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;

/**
 * @author qiang.bi
 * @date 2020/7/3 17:25
 **/
public class ZookeeperUtils {
    private static final int SESSION_TIMEOUT = 30000;
    private static final int CONNECTION_TIMEOUT = 30000;

    public static ZkUtils getZkUtilsByZkUrl(String zkUrl){
        return ZkUtils.
                apply(zkUrl, SESSION_TIMEOUT, CONNECTION_TIMEOUT,
                        JaasUtils.isZkSecurityEnabled());
    }
}
