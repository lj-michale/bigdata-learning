package com.luoj.task.learn.table.sql;

import com.luoj.task.example.pv.SensorReading;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-10
 */
public class UseWatermarkGenerator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 每秒更新一次watermark
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStreamSource<String> source = env.socketTextStream("192.168.177.211", 7777);
        SingleOutputStreamOperator<SensorReading> stream = source.map(data -> new SensorReading(
                        data.split(",")[0].trim(),
                        Long.parseLong(data.split(",")[1].trim()),
                        Double.parseDouble(data.split(",")[2].trim())
                )
        ).returns(SensorReading.class);

        stream.assignTimestampsAndWatermarks(
                new WatermarkStrategy<SensorReading>() {
                    @Override
                    public WatermarkGenerator<SensorReading> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<SensorReading>() {
                            private long maxTimesStamp = Long.MIN_VALUE;
                            // 每来一条数据，将这条数据与maxTimesStamp比较，看是否需要更新watermark
                            @Override
                            public void onEvent(SensorReading event, long eventTimestamp, WatermarkOutput output) {
                                maxTimesStamp = Math.max(event.timestamp(), maxTimesStamp);
                            }

                            // 周期性更新watermark
                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                // 允许乱序数据的最大限度为3s
                                long maxOutOfOrderness = 3000L;
                                output.emitWatermark(new Watermark(maxTimesStamp - maxOutOfOrderness));
                            }
                        };
                    }
                    // 必须指定中的timeStamp，否则报错
                }.withTimestampAssigner((element, recordTimestamp) -> element.timestamp()))
                .keyBy(SensorReading::id)
                // 创建长度为5s的事件时间窗口
                .timeWindow(Time.seconds(5))
                .apply(new WindowFunction<SensorReading, Object, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<Object> out) throws Exception {
                        System.out.println("window : [" + window.getStart() + ", " + window.getEnd() + "]");
                        ArrayList<SensorReading> list = new ArrayList<>((Collection<? extends SensorReading>) input);
                        list.forEach(out::collect);
                    }
                }).print();

        env.execute();
    }
}

