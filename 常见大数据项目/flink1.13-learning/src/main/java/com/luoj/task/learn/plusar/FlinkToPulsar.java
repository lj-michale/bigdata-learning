//package com.luoj.task.learn.plusar;
//
//
//import com.luoj.common.KafkaUtil;
//import org.apache.commons.collections.map.HashedMap;
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.runtime.state.memory.MemoryStateBackend;
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
//import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
//import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
//import org.apache.flink.streaming.util.serialization.PulsarSerializationSchemaWrapper;
//import org.apache.flink.table.api.DataTypes;
//import java.util.Map;
//import java.util.Optional;
//import java.util.Properties;
///**
// * @author lj.michale
// * @description flink消费kafka数据写入pulsar对应topic中
// * @date 2021-05-25
// */
//
//public class FlinkToPulsar {
//
//    public static void main(String[] args) throws Exception {
//
//        //创建环境，设置参数
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.disableOperatorChaining();
//        env.enableCheckpointing(60*1000*5, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new MemoryStateBackend());
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(0, 3 * 1000));
//
//        //定义kafka相关参数
//        String groupId = "kafka-group-id-1";
//        String newsTopic = "topic";
//
//        //指定kafka offset
//        Map<KafkaTopicPartition, Long> offsets = new HashedMap();
//        offsets.put(new KafkaTopicPartition("news_spider_sync_news", 0), 50000L);
//        offsets.put(new KafkaTopicPartition("news_spider_sync_news", 1), 50000L);
//
//        //从kafka获取数据
//        FlinkKafkaConsumer<ObjectNode> newsSource = KafkaUtil.getKafkaSource(newsTopic, groupId);
//        newsSource.setStartFromSpecificOffsets(offsets);
//        DataStreamSource<ObjectNode> newsDS = env.addSource(newsSource);
//        SingleOutputStreamOperator<String> valueDS = newsDS.map(jsonNodes -> jsonNodes.get("value").toString());
//
//        //定义topic
//        String topic = "persistent://tenant/namespace/topic";
//
//        //定义配置
//        Properties props = new Properties();
//        props.setProperty("topic", topic);
//        props.setProperty("partition.discovery.interval-millis", "5000");
//        props.setProperty(PulsarOptions.FLUSH_ON_CHECKPOINT_OPTION_KEY, "false");
//
//        //创建pulsar producer
//        FlinkPulsarSink<String> stringFlinkPulsarSink = new FlinkPulsarSink<>(
//                "pulsar://localhost:6650",
//                "http://localhost:18080",
//                Optional.of(topic),
//                props,
//                new PulsarSerializationSchemaWrapper.Builder<>(new SimpleStringSchema()).useAtomicMode(DataTypes.STRING()).build()
//        );
//
//        valueDS.addSink(stringFlinkPulsarSink);
//        valueDS.print();
//
//        env.execute();
//    }
//}
