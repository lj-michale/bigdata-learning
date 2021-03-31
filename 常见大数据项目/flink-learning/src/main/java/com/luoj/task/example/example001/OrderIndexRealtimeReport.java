package com.luoj.task.example.example001;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sun.deploy.util.ParameterUtil;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;


/**
 * @descripptions
 *
 * */
public class OrderIndexRealtimeReport {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings mySetting = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * 由于大屏的最大诉求是实时性，等待迟到数据显然不太现实，因此我们采用处理时间作为时间特征，并以1分钟的频率做checkpointing。
         * */
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, mySetting);

        Properties props = new Properties();
        props.put("bootstrap.servers", "172.17.11.31:9092,172.17.11.30:9092");
        props.put("zookeeper.connect", "172.17.11.31:2181,172.17.11.30:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        /**
         * 给带状态的算子设定算子ID（通过调用uid()方法）是个好习惯，能够保证Flink应用从保存点重启时能够正确恢复状态现场。为了尽量稳妥，Flink官方也建议为每个算子都显式地设定ID
         * */
        DataStream<String> sourceStream = env.addSource(new FlinkKafkaConsumer011<>(
                        "mzpns",
                        new SimpleStringSchema(),
                        props
                )).setParallelism(1)
                .name("source_kafka_" + "mzpns")
                .uid("source_kafka_" + "mzpns");

//        sourceStream.setStartFromEarliest() // - 从最早的记录开始；
//        sourceStream.setStartFromLatest() //- 从最新记录开始；
//        sourceStream.setStartFromTimestamp(null); // 从指定的epoch时间戳（毫秒）开始；
//        sourceStream.setStartFromGroupOffsets(); // 默认行为，从上次消费的偏移量进行继续消费。
        /**
         * 接下来将JSON数据转化为POJO，JSON框架采用FastJSON。
         * */
        DataStream<SubOrderDetail> orderStream = sourceStream
                .map(message -> JSON.parseObject(message, SubOrderDetail.class))
                .name("map_sub_order_detail").uid("map_sub_order_detail");


        /**
         * 统计站点指标
         * 将子订单流按站点ID分组，开1天的滚动窗口，并同时设定ContinuousProcessingTimeTrigger触发器，以1秒周期触发计算。注意处理时间的时区问题
         * */
        WindowedStream<SubOrderDetail, Tuple, TimeWindow> siteDayWindowStream = orderStream
                .keyBy("siteId")
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)));

        /**
         * 聚合函数
         * */
        DataStream<OrderAccumulator> siteAggStream = siteDayWindowStream
                .aggregate(new OrderAndGmvAggregateFunc())
                .name("aggregate_site_order_gmv").uid("aggregate_site_order_gmv");

        /**
         * 累加器类OrderAccumulator的实现很简单，看源码就大概知道它的结构了，因此不再多废话。唯一需要注意的是订单ID可能重复，所以需要用名为orderIds的HashSet来保存它。HashSet应付我们目前的数据规模还是没太大问题的，如果是海量数据，就考虑换用HyperLogLog吧。
         * 接下来就该输出到Redis供呈现端查询了。这里有个问题：一秒内有数据变化的站点并不多，而ContinuousProcessingTimeTrigger每次触发都会输出窗口里全部的聚合数据，这样做了很多无用功，并且还会增大Redis的压力。所以，我们在聚合结果后再接一个ProcessFunction
         * */
        DataStream<Tuple2<Long, String>> siteResultStream = siteAggStream
                .keyBy(0)
                .process(new OutputOrderGmvProcessFunc(), TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {}))
                .name("process_site_gmv_changed").uid("process_site_gmv_changed");

        /**
         * 就是用一个MapState状态缓存当前所有站点的聚合数据。由于数据源是以子订单为单位的，因此如果站点ID在MapState中没有缓存，或者缓存的子订单数与当前子订单数不一致，表示结果有更新，这样的数据才允许输出。
         * 最后就可以安心地接上Redis Sink了，结果会被存进一个Hash结构里。
         * */
        // 看官请自己构造合适的FlinkJedisPoolConfig
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("172.17.8.26").setDatabase(0).setPort(6379).build();
        siteResultStream
                .addSink(new RedisSink<>(jedisPoolConfig, new GmvRedisMapper()))
                .name("sink_redis_site_gmv").uid("sink_redis_site_gmv")
                .setParallelism(1);

        /**
         * 商品Top N
         * 我们可以直接复用前面产生的orderStream，玩法与上面的GMV统计大同小异。这里用1秒滚动窗口就可以了。
         * */
        WindowedStream<SubOrderDetail, Tuple, TimeWindow> merchandiseWindowStream = orderStream
                .keyBy("merchandiseId")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)));

        DataStream<Tuple2<Long, Long>> merchandiseRankStream = merchandiseWindowStream
                .aggregate(new MerchandiseSalesAggregateFunc(), new MerchandiseSalesWindowFunc())
                .name("aggregate_merch_sales").uid("aggregate_merch_sales")
                .returns(TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() { }));

        /**
         * 既然数据最终都要落到Redis，那么我们完全没必要在Flink端做Top N的统计，直接利用Redis的有序集合（zset）就行了，商品ID作为field，销量作为分数值，简单方便。不过flink-redis-connector项目中默认没有提供ZINCRBY命令的实现（必须再吐槽一次），我们可以自己加，步骤参照之前写过的那篇加SETEX的命令的文章，不再赘述。RedisMapper的写法如下。
         * 后端取数时，用ZREVRANGE命令即可取出指定排名的数据了。只要数据规模不是大到难以接受，并且有现成的Redis，这个方案完全可以作为各类Top N需求的通用实现。
         * */

        env.execute("OrderIndexRealtimeReport");

    }

    public static final class OrderAndGmvAggregateFunc implements AggregateFunction<SubOrderDetail,
            OrderAccumulator, OrderAccumulator> {

        private static final long serialVersionUID = 1L;

        @Override
        public OrderAccumulator createAccumulator() {
            return new OrderAccumulator();
        }

        @Override
        public OrderAccumulator add(SubOrderDetail record, OrderAccumulator acc) {
            if (acc.getSiteId() == 0) {
                acc.setSiteId(record.getSiteId());
                acc.setSiteName(record.getSiteName());
            }
            acc.addOrderId(record.getOrderId());
            acc.addSubOrderSum(1);
            acc.addQuantitySum(record.getQuantity());
            acc.addGmv(record.getPrice() * record.getQuantity());
            return acc;
        }

        @Override
        public OrderAccumulator getResult(OrderAccumulator acc) {
            return acc;
        }

        @Override
        public OrderAccumulator merge(OrderAccumulator acc1, OrderAccumulator acc2) {
            if (acc1.getSiteId() == 0) {
                acc1.setSiteId(acc2.getSiteId());
                acc1.setSiteName(acc2.getSiteName());
            }
            acc1.addOrderIds(acc2.getOrderIds());
            acc1.addSubOrderSum(acc2.getSubOrderSum());
            acc1.addQuantitySum(acc2.getQuantitySum());
            acc1.addGmv(acc2.getGmv());
            return acc1;
        }
    }

    public static final class OutputOrderGmvProcessFunc
            extends KeyedProcessFunction<Tuple, OrderAccumulator, Tuple2<Long, String>> {
        private static final long serialVersionUID = 1L;

        private MapState<Long, OrderAccumulator> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            state = this.getRuntimeContext().getMapState(new MapStateDescriptor<>(
                    "state_site_order_gmv",
                    Long.class,
                    OrderAccumulator.class)
            );
        }

        @Override
        public void processElement(OrderAccumulator value, Context ctx, Collector<Tuple2<Long, String>> out) throws Exception {
            long key = value.getSiteId();
            OrderAccumulator cachedValue = state.get(key);

            if (cachedValue == null || !value.getSubOrderSum().equals(cachedValue.getSubOrderSum())) {
                JSONObject result = new JSONObject();
                result.put("site_id", value.getSiteId());
                result.put("site_name", value.getSiteName());
                result.put("quantity", value.getQuantitySum());
                result.put("orderCount", value.getOrderIds().size());
                result.put("subOrderCount", value.getSubOrderSum());
                result.put("gmv", value.getGmv());
                out.collect(new Tuple2<>(key, result.toJSONString()));
                state.put(key, value);
            }
        }

        @Override
        public void close() throws Exception {
            state.clear();
            super.close();
        }
    }

    public static final class GmvRedisMapper implements RedisMapper<Tuple2<Long, String>> {
        private static final long serialVersionUID = 1L;
        private static final String HASH_NAME_PREFIX = "RT:DASHBOARD:GMV:";

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, HASH_NAME_PREFIX);
        }

        @Override
        public String getKeyFromData(Tuple2<Long, String> data) {
            return String.valueOf(data.f0);
        }

        @Override
        public String getValueFromData(Tuple2<Long, String> data) {
            return data.f1;
        }

//        @Override
//        public Optional<String> getAdditionalKey(Tuple2<Long, String> data) {
//            return Optional.of(
//                    HASH_NAME_PREFIX +
//                            "new LocalDateTime(System.currentTimeMillis()).toString(Consts.TIME_DAY_FORMAT)" +
//                            "SITES"
//            );
//        }
    }

    public static final class MerchandiseSalesAggregateFunc
            implements AggregateFunction<SubOrderDetail, Long, Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(SubOrderDetail value, Long acc) {
            return acc + value.getQuantity();
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }


    public static final class MerchandiseSalesWindowFunc
            implements WindowFunction<Long, Tuple2<Long, Long>, Tuple, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void apply(
                Tuple key,
                TimeWindow window,
                Iterable<Long> accs,
                Collector<Tuple2<Long, Long>> out) throws Exception {
            long merchId = ((Tuple1<Long>) key).f0;
            long acc = accs.iterator().next();
            out.collect(new Tuple2<>(merchId, acc));
        }

            public static final class RankingRedisMapper implements RedisMapper<Tuple2<Long, Long>> {
                private static final long serialVersionUID = 1L;
                private static final String ZSET_NAME_PREFIX = "RT:DASHBOARD:RANKING:";

                @Override
                public RedisCommandDescription getCommandDescription() {
                    return new RedisCommandDescription(RedisCommand.RPUSH, ZSET_NAME_PREFIX);
                }

                @Override
                public String getKeyFromData(Tuple2<Long, Long> data) {
                    return String.valueOf(data.f0);
                }

                @Override
                public String getValueFromData(Tuple2<Long, Long> data) {
                    return String.valueOf(data.f1);
                }

//                @Override
//                public Optional<String> getAdditionalKey(Tuple2<Long, Long> data) {
//                    return Optional.of(
//                            ZSET_NAME_PREFIX +
//                                    new LocalDateTime(System.currentTimeMillis()).toString(Consts.TIME_DAY_FORMAT) + ":" +
//                                    "MERCHANDISE"
//                    );
//                }
            }
    }

}
