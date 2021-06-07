package com.jiguang;

import java.util.Collections;
import java.util.UUID;

import com.jiguang.common.KuduUtils;
import com.jiguang.common.Spark;
import com.jiguang.common.SpringContextUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alibaba.fastjson.JSONObject;


/**
 * @author lj.michale
 * @description
 * @date 2021-06-07
 */
@SpringBootApplication
public class App {

    private static final String BEAN_CONF = "classpath:spring/spring-bean.xml";

    public static void main(String args[]) {

        try {
//            //把actx设置进去，后续可以共用
//            String[] confs = new String[] {
//                    BEAN_CONF
//            };
//            //把actx设置进去，后续可以共用
//            SpringContextUtil.setApplicationContext(new ClassPathXmlApplicationContext(confs));

            //获取spark bean
            Spark spark = (Spark) SpringContextUtil.getBean("spark");

            //获取sparkStreamingContext
            JavaInputDStream<ConsumerRecord<String, String>> stream = spark.getStream();

            //nginx日志对应的字段
            String[] columns = {"remote_addr","remote_user","time_local",
                    "request","status","body_bytes_sent","http_referer",
                    "http_user_agent","http_x_forwarded_for"};

            stream.foreachRDD(rdd -> {
                rdd.foreachPartition(records -> {
                    try {
                        while (records.hasNext()) {
                            // 解析数据
                            ConsumerRecord<String, String> consumerRecords = records.next();
                            String[] messages = consumerRecords.value() == null? (String[]) Collections.EMPTY_LIST.toArray():consumerRecords.value().split("\\|\\+\\|");
                            int length = messages.length;
                            JSONObject json = new JSONObject();

                            for (int i = 0 ; i < columns.length; i++) {
                                if (i < length) {
                                    json.put(columns[i], messages[i]);
                                }
                            }
                            //kudu表一定要有主键
                            json.put("uuid", UUID.randomUUID().toString().replace("-", ""));
                            KuduUtils.insert("impala::kudu_vip.NGINX_LOG", json);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            });

            spark.streamingStart();
            spark.streamingAwaitTermination();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
