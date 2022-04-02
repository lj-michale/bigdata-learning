package com.aurora.example.example002;

import com.aurora.example.example001.FlinkByKafkaExample2;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-04-02
 */
@Slf4j
public class FlinkByKafkaExample01 {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "KAFKA_BOOTSTRAP_SERVERS");
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"Kafka-Group-Id");

        // 考虑Kafka进行Kebores认证的情况
        String username = "";
        String password = "";

        String regex = "topic.*";
        Pattern pattern = Pattern.compile(regex);
        final HashSet<TopicPartition> partitionSet = new HashSet<>(Arrays.asList( new TopicPartition("topic-a", 0), new TopicPartition("topic-b", 5)));

        //////////////////////////////////////  消费Kafka数据
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                // Deserializer ->​StringDeserializer将 Kafka 消息值反序列化为字符串
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                // 将消息的值反序列化为字符串
                // .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(consumerProps)
                .setProperty("security.protocol", "SASL_PLAINTEXT")
                .setProperty("sasl.mechanism", "PLAIN")
                .setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";")
                // Dynamic Partition Discovery -> discover new partitions per 10 seconds
                .setProperty("partition.discovery.interval.ms", "10000")

                .setTopics("tpoic-source-name")
                // Topic-partition Subscription -> Kafka 源码提供了 3 种 topic-partition 订阅方式
                // 主题列表，订阅主题列表中所有分区的消息
                // .setTopics("topic-a", "topic-b")
                // 主题模式，从名称与提供的正则表达式匹配的所有主题订阅消息
                // .setTopicPattern(pattern)
                // 分区集，订阅提供的分区集中的分区
                // .setPartitions(partitionSet)

                // Starting Offset -> Kafka Source 能够通过指定 ​​OffsetsInitializer​​来消费从不同偏移量开始的消息
                // Start from committed offset of the consuming group, without reset strategy
                // .setStartingOffsets(OffsetsInitializer.committedOffsets())
                // Start from committed offset, also use EARLIEST as reset strategy if committed offset doesn't exist
                // .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                // Start from the first record whose timestamp is greater than or equals a timestamp
                // .setStartingOffsets(OffsetsInitializer.timestamp(1592323200L))
                // Start from earliest offset
                // .setStartingOffsets(OffsetsInitializer.earliest())
                // Start from latest offset
                // .setStartingOffsets(OffsetsInitializer.latest())
                .setStartingOffsets(OffsetsInitializer.earliest())

                .build();

        // 设置水印策略、序列化, TypeInformation类型系统...
        DataStreamSource<String> dataStreamSource = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(), "Kafka-Source");

        // DataStreamSource<String> -> DataStream<String>
        DataStream<String> dataStream = dataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).name("DataStreamSource-DataStream");

        //////////////////////////////////////  KafkaSink写入
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("brokers")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("topic-sink-name")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        dataStream.sinkTo(kafkaSink);

        env.execute("FlinkByKafkaExample01");

    }

}
