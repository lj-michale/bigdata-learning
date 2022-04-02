package com.aurora.example.example001;

import com.aurora.bean.Jason;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author lj.michale
 * @description
 * @date 2022-04-02
 */
@Slf4j
public class FlinkWithKafka {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
/*
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("brokers")
                .setTopics("input-topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
*/
        String username = "";
        String password = "";


        Properties consumerPro = new Properties();
        consumerPro.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "KAFKA_BOOTSTRAP_SERVERS");
        consumerPro.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"Kafka-Group-Id");

        KafkaSource<Jason> source = KafkaSource.<Jason>builder()
//                .setProperties(consumerPro)
                .setProperty("security.protocol", "SASL_PLAINTEXT")
                .setProperty("sasl.mechanism", "PLAIN")
                .setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";")
                // discover new partitions per 10 seconds
                .setProperty("partition.discovery.interval.ms", "10000")
                .setBootstrapServers("broker")
                .setTopics("topic")
                .setGroupId("group_id")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new MyKafkaDeserialization(true, true)))
                // 只反序列化 value
//                .setValueOnlyDeserializer(new MyDeSerializer())
                .build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStreamSource<Jason> stream = env.fromSource(source,
                WatermarkStrategy.noWatermarks(),
                "Kafka-Source",
                TypeInformation.of(Jason.class));


        env.execute("FlinkWithKafka");

    }


}
