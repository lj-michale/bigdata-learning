package com.luoj.common;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @date: 2022/1/14
 * @author: yangshibiao
 * @desc: Kafka工具类
 * Kafka Consumer
 * 1、创建 Kafka Consumer
 * 2、使用 DeserializationSchema 对kafka中的消息值进行反序列化（TypeInformationSerializationSchema、JsonDeserializationSchema、AvroDeserializationSchema 或者 自定义）
 * 3、配置 Kafka Consumer 开始消费的位置（通过 FlinkKafkaConsumer 来 setStartFromEarliest、setStartFromLatest、setStartFromTimestamp、setStartFromGroupOffsets）
 * 4、Kafka Consumer 和容错，使用 flink 的 checkpointing ，或者使用 kafka其中的topic来存储offset
 * 5、Kafka Consumer Topic 和分区发现（设置  flink.partition-discovery.interval-millis 大于0能自动发现kafka中的新分区，能使用正则表达式匹配topic）
 * 6、Kafka Consumer 提交 Offset 的行为配置，注意：提交的 offset 只是一种方法，用于公开 consumer 的进度以便进行监控
 *      6.1、 禁用 Checkpointing， 使用 Kafka client 自动定期 offset 提交功能，需要配置 enable.auto.commit 和 auto.commit.interval.ms
 *      6.2、 启用 Checkpointing，那么 offset 会保存在 checkpointing 中， 同时可以使用 consumer 上的 setCommitOffsetsOnCheckpoints(boolean) 方法来禁用或启用 offset 对 kafka broker 的提交(默认情况下，这个值是 true )，注意，在这个场景中，Properties 中的自动定期 offset 提交设置会被完全忽略。
 * 7、Kafka Consumer 和 时间戳抽取以及 watermark 发送 （使用 FlinkKafkaConsumer.assignTimestampsAndWatermarks ， 一般不用）
 *
 * Kafka Producer
 * 1、创建 Kafka Producer （下述的 getKafkaProducer 系列方法）
 * 2、SerializationSchema （自定义序列化器）
 * 3、Kafka Producer 和容错（通过 Semantic.NONE 、 Semantic.AT_LEAST_ONCE 和 Semantic.EXACTLY_ONCE 配置， 但是需要启动flink的checkpointing）
 *          当使用 Semantic.EXACTLY_ONCE 时，需要考虑到 Kafka broker 中的 transaction.max.timeout.ms （默认15分钟） 和 FlinkKafkaProducer 中的 transaction.timeout.ms（默认1小时）
 *
 */
public class MyKafkaUtil {

    public static Logger logger = LoggerFactory.getLogger(MyKafkaUtil.class);

    /**
     * 通过消费者组id 获取对应的kafka配置
     *
     * @param groupId 消费者组id
     * @return 配置对象
     */
    public static Properties getKafkaProperties(String groupId) {

        // Kakfa的参数设置
        Properties props = new Properties();

        // 集群地址 和 消费者组id（最基础的配置，必须要有）
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ModelUtil.getConfigValue("kafka.bootstrap.servers"));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 开启 FlinkKafkaConsumer 的自动分区检测，用于检测kafka中topic的分区变更
        props.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, ModelUtil.getConfigValue("kafka.flink.partition-discovery.interval-millis"));

        // 偏移量自动提交，当checkpoint关闭时会启动此参数，当checkpoint开启时，并设置了setCommitOffsetsOnCheckpoints(true)（此参数默认为true）时，会根据checkpoint的时间向kafka.broker中提交offset
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ModelUtil.getConfigValue("kafka.enable.auto.commit"));
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, ModelUtil.getConfigValue("kafka.auto.commit.interval.ms"));

        // 设置kafka消费者的事务级别
        props.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, ModelUtil.getConfigValue("kafka.isolation.level"));

        // 当在 FlinkKafkaConsumer 中没有设置消费级别，并在checkpoint中没有偏移量时，使用此设置来消费kafka中的数据
        // 具体意义：当在kafka中保存偏移量的topic中有偏移量时从偏移量消费，没有从最新开始消费（其他还可以设置earliest，从最开始的数据开始消费等）
        // 一般情况下，会直接在 FlinkKafkaConsumer 中设置消费属性
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ModelUtil.getConfigValue("kafka.auto.offset.reset"));

        // 返回参数设置对象
        return props;

    }

    /**
     * 封装kafka消费者
     *
     * @param topicName 主题名
     * @param groupId   消费者组id
     * @return 创建一个普通的Kafka消费者对象，其中的数据类型为String
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topicName, String groupId) {

        // 获取kafka的配置对象
        Properties props = MyKafkaUtil.getKafkaProperties(groupId);

        // 创建一个FlinkKafka的消费者
        return new FlinkKafkaConsumer<String>(topicName, new SimpleStringSchema(), props);
    }

    /**
     * 封装kafka消费者，从传入的时间戳开始消费
     *
     * @param topicName 主题名
     * @param groupId   消费者组id
     * @param timestamp 13为长整形时间戳
     * @return 消费者对象
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topicName, String groupId, Long timestamp) {

        // 获取kafka的配置对象
        Properties props = MyKafkaUtil.getKafkaProperties(groupId);

        // 创建一个FlinkKafka的消费者
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), props);

        // 设置从指定时间戳开始消费
        logger.info("从kafka的指定时间戳开始消费，时间戳：" + timestamp);
        consumer.setStartFromTimestamp(timestamp);

        // 返回消费者对象
        return consumer;

    }

    /**
     * 封装kafka消费者（返回一个Tuple2，其中第一个元素为kafka的value值，第二个为该消息在kafka中对应的时间戳（注意：单位为毫秒））
     *
     * @param topicName 主题名
     * @param groupId   消费者组id
     * @return 消费者对象
     */
    public static FlinkKafkaConsumer<Tuple2<String, Long>> getKafkaConsumerContainTimestamp(String topicName, String groupId) {

        // 获取kafka的配置对象
        Properties props = MyKafkaUtil.getKafkaProperties(groupId);

        // 自定义kafka的反序列化器
        KafkaDeserializationSchema<Tuple2<String, Long>> deserializationSchema = new KafkaDeserializationSchema<Tuple2<String, Long>>() {

            @Override
            public TypeInformation<Tuple2<String, Long>> getProducedType() {
                return TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                });
            }

            @Override
            public boolean isEndOfStream(Tuple2<String, Long> nextElement) {
                return false;
            }

            @Override
            public Tuple2<String, Long> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                String message = new String(record.value(), StandardCharsets.UTF_8);
                long timestamp = record.timestamp();
                return Tuple2.of(message, timestamp);
            }
        };

        // 创建一个FlinkKafka的消费者，其中包含kafka中的value和该条消息到kafka的时间
        return new FlinkKafkaConsumer<>(topicName, deserializationSchema, props);
    }

    /**
     * 封装kafka消费者，从传入的时间戳开始消费（返回一个Tuple2，其中第一个元素为kafka的value值，第二个为该消息在kafka中对应的时间戳（注意：单位为毫秒））
     *
     * @param topicName 主题名
     * @param groupId   消费者组id
     * @param timestamp 13位长整形时间戳（毫秒）
     * @return 消费者对象
     */
    public static FlinkKafkaConsumer<Tuple2<String, Long>> getKafkaConsumerContainTimestamp(String topicName, String groupId, Long timestamp) {

        // 获取kafka的配置对象
        Properties props = MyKafkaUtil.getKafkaProperties(groupId);

        // 自定义kafka的反序列化器
        KafkaDeserializationSchema<Tuple2<String, Long>> deserializationSchema = new KafkaDeserializationSchema<Tuple2<String, Long>>() {

            @Override
            public TypeInformation<Tuple2<String, Long>> getProducedType() {
                return TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                });
            }

            @Override
            public boolean isEndOfStream(Tuple2<String, Long> nextElement) {
                return false;
            }

            @Override
            public Tuple2<String, Long> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                String message = new String(record.value(), StandardCharsets.UTF_8);
                long timestamp = record.timestamp();
                return Tuple2.of(message, timestamp);
            }
        };

        // 创建一个FlinkKafka的消费者，其中包含kafka中的value和该条消息到kafka的时间
        FlinkKafkaConsumer<Tuple2<String, Long>> consumer = new FlinkKafkaConsumer<>(topicName, deserializationSchema, props);

        // 设置从指定时间戳开始消费
        logger.info("从kafka的指定时间戳开始消费，时间戳：" + timestamp);
        consumer.setStartFromTimestamp(timestamp);

        // 返回消费者对象
        return consumer;
    }

    /**
     * 获取 kafka 生产者（ 普通kafka生产者，模式为 AT_LEAST_ONCE ）
     *
     * @param topicName 主题名
     * @return 生产者对象
     */
    public static FlinkKafkaProducer<String> getKafkaProducer(String topicName) {
        return new FlinkKafkaProducer<>(ModelUtil.getConfigValue("kafka.bootstrap.servers"), topicName, new SimpleStringSchema());
    }

    /**
     * 获取 Kafka 生产者，动态指定多个不同主题，并使用精确一次语议
     *
     * @param serializationSchema 序列化模式
     * @param <T>                 来源数据类型
     * @return FlinkKafkaProducer
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducerExactlyOnce(KafkaSerializationSchema<T> serializationSchema, String defaultTopicName) {

        Properties prop = new Properties();
        // kafka的 bootstrap.servers
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ModelUtil.getConfigValue("kafka.bootstrap.servers"));
        //如果 10 分钟没有更新状态，则超时( 默认超时时间是1分钟)，表示已经提交事务到kafka，但10分钟还没有上传数据，结束事务
        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(10 * 60 * 1000));
        // 配置生产者的kafka的单条消息的最大大小（ 默认为1M，这里设置为10M ）
        prop.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(10 * 1000 * 1000));

        return new FlinkKafkaProducer<>(defaultTopicName, serializationSchema, prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

    }

    /**
     * 获取 Kafka 生产者，并使用精确一次语议
     *
     * @param topicName 主题名
     * @param <T>       来源数据类型
     * @return FlinkKafkaProducer
     */
    public static <T extends String> FlinkKafkaProducer<T> getKafkaProducerExactlyOnce(String topicName) {

        return MyKafkaUtil.getKafkaProducerExactlyOnce(
                new KafkaSerializationSchema<T>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(T t, @Nullable Long aLong) {
                        return new ProducerRecord<>(topicName, JSON.toJSONBytes(t));
                    }
                },
                ModelUtil.getConfigValue("kafka.topic.default")
        );
    }
}
