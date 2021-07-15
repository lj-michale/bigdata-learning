package com.luoj.common;

import java.util.Properties;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-02
 */
public class PropConfigUtil {


    public final static String BROKER_LIST = "localhost:9092";
    public final static String ZOOKEEPER_QUORUM = "venn";
    public final static String ZOOKEEPER_PORT = "2180";
    public final static String ZOOKEEPER_ZNODE_PARENT = "localhost:9092";
    public final static String CHECK_POINT_DATA_DIR = "file:///out/checkpoint";

    public static Properties getProperties(){
        Properties prop = new Properties();
        if(prop == null){
            prop = new Properties();
            prop.put("bootstrap.servers", BROKER_LIST);
            prop.put("request.required.acks", "-1");
            prop.put("auto.offset.reset", "latest");
            prop.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            prop.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            prop.put("group.id", "venn");
            prop.put("client.id", "venn");
        }
        return prop;
    }

}
