package com.luoj.task.learn.kafka;

/**
 * @author lj.michale
 * @description 普通的消费
 * @date 2021-05-26
 */
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSource {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        //Kafka源
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","114.116.219.197:5008");
        properties.setProperty("group.id","test");

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>("flinktestkafka", new SimpleStringSchema(), properties);
        DataStreamSource<String> streamFromKafka = env.addSource(flinkKafkaConsumer);
        SingleOutputStreamOperator<KafkaElement> mapresult = streamFromKafka.map(new MapFunction<String, KafkaElement>() {
            @Override
            public KafkaElement map(String value) throws Exception {
                String[] sp = value.split(",");
                System.out.println(sp[0]+"  "+sp[1]+"  "+sp[2]);
                return new KafkaElement(sp[0], sp[1], Integer.valueOf(sp[2]));
            }
        });

        try {
            env.execute("this is kafkaflink job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
