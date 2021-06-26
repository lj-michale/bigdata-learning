package com.luoj.task.learn.state.example003;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author lj.michale
 * @description
 * Broadcast State实现流处理配置实时更新
 *
 * Broadcast State是Flink支持的一种Operator State。
 * 使用Broadcast State，可以在Flink程序的一个Stream中输入数据记录，然后将这些数据记录广播（Broadcast）到下游的每个Task中，使得这些数据记录能够为所有的Task所共享，
 * 比如一些用于配置的数据记录。这样，每个Task在处理其所对应的Stream中记录的时候，读取这些配置，来满足实际数据处理需要。
 *
 * 另外，在一定程度上，Broadcast State能够使得Flink Job在运行过程中与外部的其他系统解耦合。
 * 比如，通常Flink会使用YARN来管理计算资源，使用Broadcast State就可以不用直接连接MySQL数据库读取相关配置信息了，
 * 也无需对MySQL做额外的授权操作。因为在一些场景下，会使用Flink on YARN部署模式，将Flink Job运行的资源申请和释放交给YARN去管理，
 * 那么就存在Hadoop集群节点扩缩容的问题，如新加节点可能需要对一些外部系统的访问，
 * 如MySQL等进行连接操作授权，如果忘记对MysQL访问授权，Flink Job被调度到新增的某个新增节点上连接并读取MySQL配置信息就会出错。
 *
 * @date 2021-06-25
 */
@Slf4j
public class ConfigRealTimeChangeByBroadcastState {

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

        log.info("Input args: " + Arrays.asList(args));
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (parameterTool.getNumberOfParameters() < 5) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-event-topic <topic> --input-config-topic <topic> --output-topic <topic> " +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        CheckpointConfig config = bsEnv.getCheckpointConfig();
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        config.setCheckpointInterval(1 * 60 * 60 * 1000);
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // make parameters available in the web interface
        bsEnv.getConfig().setGlobalJobParameters(parameterTool);
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        bsEnv.getCheckpointConfig().setCheckpointStorage("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint");

        // 创建用户行为事件Stream
        // 创建一个用来处理用户在App上操作行为事件的Stream，并且使用map进行转换，使用keyBy来对Stream进行分区
        FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010<>(("eventDetails"), new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromGroupOffsets();

        // create customer user event stream
        final FlinkKafkaConsumer010 kafkaUserEventSource = new FlinkKafkaConsumer010<>(
                parameterTool.getRequired("input-event-topic"),
                new SimpleStringSchema(), parameterTool.getProperties());

       // (userEvent, userId)
        final KeyedStream<UserEvent, String> customerUserEventStream = bsEnv
                .addSource(kafkaUserEventSource)
                .map(new MapFunction<String, UserEvent>() {
                    @Override
                    public UserEvent map(String s) throws Exception {
                        return UserEvent.buildEvent(s);
                    }
                })
                .assignTimestampsAndWatermarks(new CustomWatermarkExtractor(Time.hours(24)))
                .keyBy(new KeySelector<UserEvent, String>() {
                    @Override
                    public String getKey(UserEvent userEvent) throws Exception {
                        return userEvent.getUserId();
                    }
                });
//        {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12_09:27:11","data":{"productId":196}}
//        {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"ADD_TO_CART","eventTime":"2018-06-12_09:43:18","data":{"productId":126}}
//        {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12_09:27:11","data":{"productId":126}}
//        {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"PURCHASE","eventTime":"2018-06-12_09:30:28","data":{"productId":196,"price":600.00,"amount":600.00}}

        // 创建配置事件Stream
        // 创建一个用来动态读取Kafka Topic中配置的Broadcast Stream，它是基于Flink的Broadcast State特性
        // create dynamic configuration event stream
        final FlinkKafkaConsumer010 kafkaConfigEventSource = new FlinkKafkaConsumer010<>(
                parameterTool.getRequired("input-config-topic"),
                new SimpleStringSchema(), parameterTool.getProperties());

        final BroadcastStream<Config> configBroadcastStream = bsEnv
                .addSource(kafkaConfigEventSource)
                .map(new MapFunction<String, Config>() {
                    @Override
                    public Config map(String value) throws Exception {
                        return Config.buildConfig(value);
                    }
                }).broadcast(configStateDescriptor);

        // 连接两个Stream并实现计算处理
        // 我们需要把最终的计算结果保存到一个输出的Kafka Topic中，所以先创建一个FlinkKafkaProducer010
        final FlinkKafkaProducer010 kafkaProducer = new FlinkKafkaProducer010(
                parameterTool.getRequired("output-topic"),
                (SerializationSchema) new EvaluatedResultSchema(),
                parameterTool.getProperties());

        // 再调用customerUserEventStream的connect()方法连接到configBroadcastStream，从而获取到configBroadcastStream中对应的配置信息，进而处理实际业务逻辑，
        // connect above 2 streams
//        DataStream<EvaluatedResult> connectedStream = customerUserEventStream
//                .connect(configBroadcastStream)
//                .process(new ConnectedBroadcastProcessFuntion());
//        connectedStream.addSink(kafkaProducer);

        bsEnv.execute("ConfigRealTimeChangeByBroadcastState");

    }

    private static final MapStateDescriptor<String, Config> configStateDescriptor =
            new MapStateDescriptor<>(
                    "configBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<Config>() {}));

    // Flink处理过程中使用的TimeCharacteristic，我们使用了TimeCharacteristic.EventTime，
    // 也就是根据事件本身自带的时间来进行处理，用来生成Watermark时间戳，对应生成Watermark的实现我们使用了BoundedOutOfOrdernessTimestampExtractor，
    // 即设置一个容忍事件乱序的最大时间长度
    private static class CustomWatermarkExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserEvent> {
        public CustomWatermarkExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }
        @Override
        public long extractTimestamp(UserEvent element) {
            return element.getEventTimestamp();
        }
    }

    // {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"PURCHASE","eventTime":"2018-06-12_09:30:28","data":{"productId":196,"price":600.00,"amount":600.00}}
    @Data
    public static class UserEvent{

        private String userId;

        private long eventTimestamp;

        public static UserEvent buildEvent(String s){
            UserEvent userEvent = new UserEvent();
            return userEvent;
        }

    }

    // {"channel":"APP","registerDate":"2018-01-01","historyPurchaseTimes":0,"maxPurchasePathLength":3}
    @Data
    public static class Config{

        private String channel;

        private String registerDate;

        private int historyPurchaseTimes;

        private int maxPurchasePathLength;

        public static Config buildConfig(String s) {
            Config config = new Config();
            return config;
        }
    }

    // {"userId":"a9b83681ba4df17a30abcf085ce80a9b","channel":"APP","purchasePathLength":9,"eventTypeCounts":{"ADD_TO_CART":1,"PURCHASE":1,"VIEW_PRODUCT":7}}
    @Data
    public static class EvaluatedResultSchema{

        private String userId;
    }

    @Data
    public static class EvaluatedResult{

    }

//    new ProcessWindowFunction<Tuple2<String, Integer>,String, String, TimeWindow>() {
//        @Override
//        public void process(String s, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<String> out) throws Exception {
//            long count = 0;
//            for (Tuple2<String, Integer> in: iterable) {
//                count++;
//            }
//
//            out.collect("aaaaaaaaaaaaa");
//        }
//    }

}
