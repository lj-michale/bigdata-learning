package com.luoj.task.learn.datastream;

import com.luoj.task.learn.window.DataStreamToWindowsCount;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-26
 */
@Slf4j
public class DataStreamUserDefinedFunctionExample {

    public static void main(String[] args) throws Exception {

        ParameterTool paramTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint");
        env.setStateBackend(new HashMapStateBackend());

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<Order> inputOrderDS = env.addSource(new RichSourceFunction<Order>() {
            private Boolean isRunning = true;

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random=new Random();
                while(isRunning) {
                    Order order = new Order(
                            UUID.randomUUID().toString(),
                            random.nextInt(3),
                            random.nextInt(101),
                            System.currentTimeMillis()
                    );
                    TimeUnit.SECONDS.sleep(1);
                    ctx.collect(order);
                }
            }

            @Override
            public void cancel() {
                isRunning=false;
            }

        });

        /**
         * User-Defined Functions: Implementing an interface、Anonymous classes、Java 8 Lambdas、Rich functions
         * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/datastream/user_defined_functions/
         */
//        inputOrderDS.map(new MyMapFunction()).print();
//        inputOrderDS.map(new MyMapFunction2()).print();

        /**
         * Accumulators & Counters
         * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/datastream/user_defined_functions/
         */
        IntCounter numLines = new IntCounter();
        numLines.add(1);

        /**
         * Window
         * KeyedStream → WindowedStream
         */
        inputOrderDS.keyBy( value -> value.userId).window(TumblingEventTimeWindows.of(Time.seconds(5)));

        /**
         * WindowAll
         * DataStreamStream → AllWindowedStream
         */
        inputOrderDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));

        /**
         * Window Apply
         * WindowedStream → DataStream
         * AllWindowedStream → DataStream
         * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/datastream/operators/overview/
         */
        AllWindowedStream allWindowedStream = inputOrderDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));

        /**
         * Windows窗口计算
         *   Tumbling Windows
         *   Sliding Windows
         *   Session Windows
         *   Global Windows
         *
         *   Window Functions
         *
         *   Triggers
         *
         *   Evictors
         *
         *   Allowed Lateness
         */
        DataStream dataStream = inputOrderDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp)->event.createTime))
                .keyBy(value -> value.userId)
                // Sliding Windows 计算
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                // Tumbling Windows 计算
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Order>() {
                        @Override
                        public Order reduce(Order order1, Order order2) throws Exception {
                            return new Order(order1.getOrderId(),
                                    order1.getUserId(),
                            order1.getMoney() + order2.getMoney(),
                                    order1.getCreateTime());
                        }
                });

        dataStream.print();


        /**
         * Window Join
         * stream.join(otherStream)
         *     .where(<KeySelector>)
         *     .equalTo(<KeySelector>)
         *     .window(<WindowAssigner>)
         *     .apply(<JoinFunction>)
         *
         * 1. Tumbling Window Join
         * 2. Sliding Window Join
         * 3. Session Window Join
         * 4. Interval Join
         */


        env.execute("DataStreamUserDefinedFunctionExample");

    }

    static class MyMapFunction implements MapFunction<Order, Order> {
        @Override
        public Order map(Order order) throws Exception {
            String orderId = order.getOrderId();
            Integer userId = order.getUserId() + 10;
            Integer money = order.getMoney() * 10;
            Long createTime = order.getCreateTime();
            return new Order(orderId, userId, money, createTime);
        }
    }

    static class MyMapFunction2 extends RichMapFunction<Order, Order> {
        @Override
        public Order map(Order order) throws Exception {
            String orderId = order.getOrderId();
            Integer userId = order.getUserId() + 10;
            Integer money = order.getMoney() * 10;
            Long createTime = order.getCreateTime();
            return new Order(orderId, userId, money, createTime);
        }
    }



    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order{
        public String orderId;
        public Integer userId;
        public Integer money;
        public Long createTime;
    }


}
