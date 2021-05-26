package com.luoj.task.learn.kafka;

/**
 * @author lj.michale
 * @description  指定分区和偏移量消费
 * @date 2021-05-26
 */
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaFlinkTest1 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        //Kafka源
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","114.116.219.197:5008");
        properties.setProperty("group.id","KafkaFlinkTest1a");

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>("flinktestkafka", new SimpleStringSchema(), properties);
        Map<KafkaTopicPartition,Long> specificStartOfssets = new HashMap<>();
        specificStartOfssets.put(new KafkaTopicPartition("flinktestkafka",0), 10L);
//        specificStartOfssets.put(new KafkaTopicPartition("flinktestkafka",1),0L);
//        specificStartOfssets.put(new KafkaTopicPartition("flinktestkafka",2),0L);
        flinkKafkaConsumer.setStartFromSpecificOffsets(specificStartOfssets);
        DataStreamSource<String> streamFromKafka = env.addSource(flinkKafkaConsumer);

        streamFromKafka.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String value) throws Exception {
                String[] sp = value.split(",");
                System.out.println(sp[0]+"  "+sp[1]+"  "+sp[2]);
                return null;
            }
        });

        try {
            env.execute("this is kafkaflink job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}