package com.bidata.example.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;

import scala.Tuple3;
/**
 * @author lj.michale
 * @description
 * @date 2021-05-31
 */
public class SparkKafkaManager {

    private static final Logger logger = LoggerFactory.getLogger(SparkKafkaManager.class);

    public static void main(String[] args) {

        final SparkSession spark = SparkSession.builder().appName("TimeWindow").getOrCreate();
        spark.streams().addListener(new CustomStreamingQueryListener());
        String brokers = "172.16.10.91:9092,172.16.10.92:9092,172.16.10.93:9092";

        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("subscribe", "topic_name")
                .option("kafka.bootstrap.servers", brokers)
                .load();

        spark.close();

    }
}
