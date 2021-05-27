package com.luoj.task.example.userbehavior;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hive.reshaded.parquet.org.apache.thrift.annotation.Nullable;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author lj.michale
 * @description 每五分钟实时计算小时内用户行为数据
 * @date 2021-05-27
 */
public class UserBehavingInfoTask {

    /**
     * kafka需要的属性：zookeeper的位置
     */
    private static Properties properties;

    static {
        properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "event");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010<>(("eventDetails"), new SimpleStringSchema(), properties);

        //flink会自动保存kafka 偏移量作为状态
        // start from the earliest record possible
        kafkaConsumer.setStartFromGroupOffsets();

        // 接收kafka数据，转为UserBehavingInfo 对象
        SingleOutputStreamOperator input = env.addSource(kafkaConsumer)
                .map(s -> JSON.parseObject(s.toString(), UserBehavingInfo.class))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavingInfo>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(UserBehavingInfo UserBehavingInfo) {
                        System.out.println("mark:" + (UserBehavingInfo.getTime() - 5*1000L));
                        return UserBehavingInfo.getTime();
                    }
                }).setParallelism(1);

        // 将用户行为数据异步插入mysql
        // 异步IO 获取mysql数据, timeout 时间 1s，容量 10(超过10个请求，会反压上游节点) unorderedWait返回结果无顺序(如果是事件时间 实则会根据watermark排序) orderedWait返回结果有序(fifo)
        // 超过10个请求，会反压上游节点 反压机制来抑制上游数据的摄入
        AsyncDataStream.unorderedWait(input, new AsyncInsertUserBehavior2Mysql(), 1000, TimeUnit.MICROSECONDS, 10);
        SingleOutputStreamOperator filterClick = input.filter(new FilterFunction<UserBehavingInfo>() {
            @Override
            public boolean filter(UserBehavingInfo userBehavingInfo) throws Exception {
                return "click".equals(userBehavingInfo.getBehavior());
            }
        });

        //创建迟到数据侧输出流
        final org.apache.flink.util.OutputTag lateOutputUserBehavior = new org.apache.flink.util.OutputTag<String>("late-userBehavior-data"){};
        SingleOutputStreamOperator<Tuple2<String, String>> aggregateUserClick = filterClick
                .keyBy(new UserBehavingInfoKeyByGood())
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)
                 // Time.hours(1), Time.minutes(5)
                )).allowedLateness(Time.hours(1))
                .sideOutputLateData(lateOutputUserBehavior)
                // 增量计算用户点击数量
                .aggregate(new UserBehavorCountAggregateUtils(), new UserBehavorCountWindowFunction());

        aggregateUserClick.print();

        // 迟到数据 迟到数据不会触发窗口 存入数据库
        AsyncDataStream.unorderedWait(aggregateUserClick.getSideOutput(lateOutputUserBehavior), new AsyncInsertUserBehavior2Mysql(), 1000, TimeUnit.MICROSECONDS, 10);

        //输入到redis中 rank:click
        FlinkJedisPoolConfig redis = new FlinkJedisPoolConfig.Builder().setDatabase(1).setHost("192.168.219.128").setPort(6379).setPassword("redis").build();
        aggregateUserClick.addSink(new RedisSink<>(redis, new UserBehaviorRedisMapper()));

        // 定义计算结果到 ProducerRecord 的转换
        // 这里可以使用 Lambda 表达式代替
        KafkaSerializationSchema<String> kafkaSerializationSchema = new KafkaSerializationSchema<String>() {
            private static final long serialVersionUID = 9079854407359463975L;
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
                return new ProducerRecord<>("out-topic", s.getBytes());
            }
        };

        // 定义Flink Kafka生产者：发送消息为精准一次
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("out-topic",
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

        // 字符串转小写；
        // 写入Kafka
        aggregateUserClick.map(new MapFunction<Tuple2<String, String>, String>() {
            @Override
            public String map(Tuple2<String, String> stringStringTuple2) throws Exception {
                String str = stringStringTuple2.f0 + stringStringTuple2.f1;
                return null;
            }
        }).map(String::toLowerCase).addSink(kafkaProducer);

        env.execute("UserBehavingInfoTask");
    }

    /**
     * 获取Flink-Kafka消费者对象
     * @return FlinkKafkaConsumer
     */
    private static FlinkKafkaConsumer<String> getConsumer(){
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("in-topic", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        return consumer;
    }

}
