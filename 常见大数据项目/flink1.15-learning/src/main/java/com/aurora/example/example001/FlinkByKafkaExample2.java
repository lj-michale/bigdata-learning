package com.aurora.example.example001;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @descri  Flink1.14.4新版KafkaSource消费kafka数据并存入kafka示例
 *
 * @author lj.michale
 * @date 2022-04-02
 */
public class FlinkByKafkaExample2 {

    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties consumerPro = new Properties();
        consumerPro.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "KAFKA_BOOTSTRAP_SERVERS");
        consumerPro.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"Kafka-Group-Id");

        KafkaSource<Test> testKafkaSource = KafkaSource.<Test>builder()
                .setProperties(consumerPro)
                .setTopics("Topic-Name")
//                .setValueOnlyDeserializer(new KafkaSourceSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStreamSource<Test> testDataStream = env.fromSource(testKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-Source",
                TypeInformation.of(Test.class));

        DataStream<String> stream = testDataStream.map(new MapFunction<Test, String>() {
            @Override
            public String map(Test value) throws Exception {
                return String.valueOf(value)+ "_" + String.valueOf(value.name);
            }
        });

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("brokers")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("topic-name")
                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build()
                )
                .build();

        stream.sinkTo(sink);

    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    static class Test{
        private int id;
        private int name;
    }
}
