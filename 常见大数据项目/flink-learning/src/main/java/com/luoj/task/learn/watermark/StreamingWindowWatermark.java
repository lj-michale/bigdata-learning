package com.luoj.task.learn.watermark;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author lj.michale
 * @description
 * @date 2021-04-11
 */
@Slf4j
public class StreamingWindowWatermark {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings mySetting = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, mySetting);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // Get Data
        DataStreamSource<String> dataDS = env.socketTextStream("", 9000);

        DataStreamSource<Tuple2<String, Long>> inputMap = (DataStreamSource<Tuple2<String, Long>>) dataDS.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] arr = s.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });

        // 抽取timestamp & 生成Watermark
        DataStreamSource<Tuple2<String, Long>> waterMarkStream = (DataStreamSource<Tuple2<String, Long>>) inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            Long currentMaxTimestamp = 0L;
            final Long maxOutOfOrderness = 1000L;    // 最大乱序允许时间10s
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long l) {
                Long timestamp = element._2;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                log.info("key:{}", element._1, "eventTime:{}", sdf.format(element._2), "currentMaxTimestamp:{]", sdf.format(currentMaxTimestamp), "watermark:{}",sdf.format(getCurrentWatermark().getTimestamp()));
                return timestamp;
            }
        });

        SingleOutputStreamOperator<Object> windowDStream = waterMarkStream.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new WindowFunction<Tuple2<String, Long>, Object, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> input, Collector<Object> out) throws Exception {
                String key = tuple.toString();
                List<String> arrarList = new ArrayList<>();
                Iterator<Tuple2<String, Long>> it = input.iterator();
                while (it.hasNext()) {
                    Tuple2<String, Long> next = it.next();
                    arrarList.add(next._1);
                }
                Collections.sort(arrarList);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                String result = key + "," + arrarList.size() + "," + sdf.format(arrarList.get(0)) + "," + sdf.format(arrarList.size() - 1) + "," + sdf.format(timeWindow.getStart()) + "," + sdf.format(timeWindow.getEnd());
                out.collect(result);
            }
        });

        windowDStream.print();

        env.execute("StreamingWindowWatermark");

    }

}
