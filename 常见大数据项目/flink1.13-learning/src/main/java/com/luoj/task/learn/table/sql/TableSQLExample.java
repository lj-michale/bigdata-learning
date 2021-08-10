package com.luoj.task.learn.table.sql;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.util.Random;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author lj.michale
 * @description
 * 注：可以对每个事件生成一个水印。但是，由于每个水印都会引起下游的一些计算，因此过多的水印会降低性能。
 * @date 2021-08-10
 */
@Slf4j
public class TableSQLExample {

    public static final int OUT_OF_ORDERNESS = 1000;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        // Checkpoint
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint");

        /**
         * @descr WatermarkGenerator: 有两种不同的 Watermark 生成方式：periodic（周期的）和 punctuated（带标记的）。
         */
        SingleOutputStreamOperator<Event> source = env.addSource(new OutOfOrderEventSource())
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {
                    @Override
                    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        // 根据事件，或周期性地生成 watermark
                        return new WatermarkGenerator<Event>() {
                            private long maxTimestamp;
                            private long maxTimeLag = 2000;
                            /**
                             * 为每个事件调用，允许 watermark generator 检查并记住事件时间戳
                             * 或基于事件本身发出 watermark
                             */
                            @Override
                            public void onEvent(Event event, long eventTimestamp, WatermarkOutput watermarkOutput) {
                                maxTimestamp = Math.max(maxTimestamp, event.eventTime);
                            }
                            /**
                             * 定期调用，可能会发出新的 watermark，也可能不会
                             * 调用间隔取决于 ExecutionConfig#getAutoWatermarkInterval() 设置的值
                             */
                            @Override
                            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                                watermarkOutput.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
                            }
                        };
                    }
                });

       tEnv.createTemporaryView("t_source", source, $("eventTime").rowtime());
       Table sorted = tEnv.sqlQuery("select eventTime from t_source order by eventTime");
       DataStream<Row> rowDataStream = tEnv.toAppendStream(sorted, Row.class);
       rowDataStream.print();

       env.execute("TableSQLExample");

    }

    public static class Event {

        Long eventTime;

        Event() {
            //构造生成带有事件时间的数据(乱序)
            this.eventTime = Instant.now().toEpochMilli() + (new Random().nextInt(OUT_OF_ORDERNESS));
        }

        @Override
        public String toString() {
            return "Event{" +
                    "eventTime=" + eventTime +
                    '}';
        }
    }

    /**
     * 数据源，这里不断的造数据
     */
    private static class OutOfOrderEventSource extends RichSourceFunction<Event> {

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            while (running) {
                ctx.collect(new Event());
                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    //////////////////////////// periodic 周期水印 ////////////////////////////
    /**
     * 事件时间 watermark
     * 事件到达时在一定程度上是无序的，某个时间戳 t 的最后达到元素相比时间戳 t 的最早到达元素，最大延迟 n 毫秒。
     */
    public class BoundedOutOfOrdernessGenerator implements WatermarkGenerator<Event> {
        // 允许数据的延迟，3.5 seconds
        private final long maxOutOfOrderness = 3500;

        private long currentMaxTimestamp;

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
        }
    }

    /**
     * 处理时间 watermark
     * 生成的 watermark 比处理时间滞后固定时间长度。
     */
    public class TimeLagWatermarkGenerator implements WatermarkGenerator<Event> {

        private final long maxTimeLag = 5000;

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            // 处理时间不关心事件
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
        }
    }

    ////////////////////////////  punctuated 标记水印 ////////////////////////////

    public class PunctuatedAssigner implements WatermarkGenerator<Event> {

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            // 事件只要带有特殊标记，就发出 watermark
//            if (event.hasWatermarkMarker()) {
//                output.emitWatermark(new Watermark(event.getWatermarkTimestamp()));
//            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // to do nothing
        }
    }


    /**
     * 时间水印
     */
    private static class TimestampsAndWatermarks implements WatermarkGenerator<Event> {
        private long maxTimestamp;
        private long delay = 3000;

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput watermarkOutput) {
            maxTimestamp = Math.max(maxTimestamp, event.eventTime);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            watermarkOutput.emitWatermark(new Watermark(maxTimestamp - delay));
        }
    }

}
