package com.luoj.task.learn.kafka;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @program flink-demo
 * @description: 演示flinkconnectors-kafkaConsumer-相当于source + kafka producer/sink
 * @author: erainm
 * @create: 2021/03/04 10:43
 */
public class Kafka_Sink_Demo {

    public static void main(String[] args) throws Exception {

        // TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // TODO 2.source
        // 准备kafka连接参数
        Properties props_consumer = new Properties();
        props_consumer.setProperty("bootstrap.servers", "172.17.11.31:9092,172.17.11.30:9092");//集群地址
        props_consumer.setProperty("group.id", "flink");//消费组id
        props_consumer.setProperty("auto.offset.reset","latest");//有offset时从offset记录位置开始消费，没有offset时，从最新的消息开始消费
        props_consumer.setProperty("flink.partition-discovery.interval-millis","5000");//会开启一个后台线程每隔5s检测一下Kafka的分区情况，实现动态分区检测
        props_consumer.setProperty("enable.auto.commit", "true");//自动提交（默认提交到主题，后面会存储到checkPoint中和默认主题中）
        props_consumer.setProperty("auto.commit.interval.ms", "2000");//自动提交的时间间隔

        // 使用连接参数创建flinkKafkaConsumer/KafkaSource
        FlinkKafkaConsumer kafkaSource = new FlinkKafkaConsumer<String>("jiguang_001", new SimpleStringSchema(), props_consumer);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        // TODO 3.transformation
        SingleOutputStreamOperator<String> etlDS = kafkaDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.contains("success");
            }
        });

        etlDS.print();

        // TODO 4.sink
        Properties props_producer = new Properties();
        props_producer.setProperty("bootstrap.servers", "node1:9092");

        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<String>(
                "flink_kafka2",                  // target topic
                new SimpleStringSchema(),        // serialization schema
                props_producer);
        etlDS.addSink(kafkaSink);

        // TODO 5.execute
        env.execute();
    }
}