package com.luoj.task.learn.watermark.example03;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.Iterator;
/**
 * @author
 * ​​env.getConfig().setAutoWatermarkInterval(1000L);​​每隔一秒去自动emitWatermark，
 * ​​TumblingEventTimeWindows.of(Time.seconds(4))​​滚动窗口为4s
 * ​​private long maxOutOfOrderness = 3000L;​​ 允许的最大延迟时间3s
 *
 * 测试数据：
 * 01,1635867066000
 * 01,1635867067000
 * 01,1635867068000
 * 01,1635867069000
 * 01,1635867070000
 * 01,1635867071000
 *
 * @date
 */
public class Demo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置自动水印发射的间隔
        env.getConfig().setAutoWatermarkInterval(1000L);
        env.setParallelism(1);
        DataStreamSource<String> sourceDs = env.socketTextStream("gcw1", 10086);

        SingleOutputStreamOperator<Tuple2<String, Long>> mapDs = sourceDs.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[0], Long.valueOf(split[1]));
            }
        });
        //周期性 发射watermark
        SingleOutputStreamOperator<Tuple2<String, Long>> watermarks = mapDs.assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple2<String, Long>>() {
            @Override
            public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<Tuple2<String, Long>>() {
                    private long maxTimeStamp = 0L;
                    private long maxOutOfOrderness = 3000L; //允许的最大延迟时间
                    @Override
                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                        //每次来一条数据就会触发一次
                        maxTimeStamp = Math.max(maxTimeStamp, event.f1);
                    }
                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        //周期性 发射watermark
                        output.emitWatermark(new Watermark(maxTimeStamp - maxOutOfOrderness));
                    }
                };
            }
        }.withTimestampAssigner(((element, recordTimestamp) -> element.f1)));
        watermarks.keyBy(x -> x.f0).window(TumblingEventTimeWindows.of(Time.seconds(4)))
                .apply(new WindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        Iterator<Tuple2<String, Long>> iterator = input.iterator();
                        int count = 0;
                        while (iterator.hasNext()) {
                            count++;
                            iterator.next();
                        }
                        out.collect(window.getStart() + "->" + window.getEnd() + " " + s + ":" + count);
                    }
                }).print();
        env.execute();
    }
}