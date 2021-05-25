package com.luoj.common;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-25
 */
public class KafkaUtil {

    public static FlinkKafkaConsumer<ObjectNode> getKafkaSource(String newsTopic, String groupId) {


        return null;
    }

}
