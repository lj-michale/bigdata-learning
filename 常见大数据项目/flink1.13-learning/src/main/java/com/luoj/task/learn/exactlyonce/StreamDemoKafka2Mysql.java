package com.luoj.task.learn.exactlyonce;

import com.luoj.task.learn.connector.mysql.MySqlTwoPhaseCommitSink;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.net.URI;
import java.util.Properties;

/**
 * @author lj.michale
 * @description  消费kafka消息，sink(自定义)到mysql中，保证kafka to mysql 的Exactly-Once
 * @date 2021-05-27
 */
@SuppressWarnings("all")
public class StreamDemoKafka2Mysql {

    private static final String topic_ExactlyOnce = "mysql-exactly-Once-4";
    private static final int FILE_SIZE_THESHOLD = 300;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度,为了方便测试，查看消息的顺序，这里设置为1，可以更改为多并行度
        env.setParallelism(1);
        //checkpoint的设置
        //每隔10s进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(10000);
        //设置模式为：exactly_one，仅一次语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //同一时间只允许进行一次检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend,将检查点保存在hdfs上面，默认保存在内存中。这里先保存到本地
//        env.getCheckpointConfig().setCheckpointStorage("hdfs:///checkpoints-data/");
        env.getCheckpointConfig().setCheckpointStorage(
                new FileSystemCheckpointStorage(new URI("hdfs:///checkpoints-data/"), FILE_SIZE_THESHOLD));

        //设置kafka消费参数
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "zzy:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group2");
        //kafka分区自动发现周期
        props.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "3000");

        /*SimpleStringSchema可以获取到kafka消息，JSONKeyValueDeserializationSchema可以获取都消息的key,value，metadata:topic,partition，offset等信息*/
        FlinkKafkaConsumer011<ObjectNode> kafkaConsumer011 = new FlinkKafkaConsumer011<>(topic_ExactlyOnce, new JSONKeyValueDeserializationSchema(true), props);

        //加入kafka数据源
        DataStreamSource<ObjectNode> streamSource = env.addSource(kafkaConsumer011);
//        System.out.println("streamSource:" + streamSource.print());
        streamSource.print();
        //数据传输到下游
        streamSource.addSink(new MySqlTwoPhaseCommitSink()).name("MySqlTwoPhaseCommitSink");
        //触发执行
        env.execute(StreamDemoKafka2Mysql.class.getName());
    }
}