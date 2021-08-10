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

        SingleOutputStreamOperator<Event> source = env.addSource(new OutOfOrderEventSource())
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {
                    @Override
                    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<Event>() {
                            private long maxTimestamp;
                            private long delay = 0;
                            @Override
                            public void onEvent(Event event, long eventTimestamp, WatermarkOutput watermarkOutput) {
                                maxTimestamp = Math.max(maxTimestamp, event.eventTime);
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                                watermarkOutput.emitWatermark(new Watermark(maxTimestamp - delay));
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
