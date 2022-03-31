package com.aurora.feature.datastream;

import com.aurora.generate.WordCountSource1ps;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * @descri  Fink1.14.4 DataStream程序
 *
 * @author lj.michale
 * @date 2022-03-31
 */
@Slf4j
public class FlinkDataStreamDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.of(2, TimeUnit.SECONDS) ));
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.15-learning\\checkpoint");

        SingleOutputStreamOperator<Tuple2<String, Integer>> result1 = env.addSource(new WordCountSource1ps())
                .flatMap(new FlatMapFunction1())
                .keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1);

        DataStream<Tuple2<String, Integer>> result2 = env.addSource(new WordCountSource1ps())
                .flatMap(new FlatMapFunction1())
                // 已经过时的方法
                .keyBy(0)
                .sum(1);

//        SingleOutputStreamOperator<Tuple2<String, Integer>> result3 = env.addSource(new WordCountSource1ps())
//                .flatMap(new FlatMapFunction1())
//                .keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
//                    @Override
//                    public Object getKey(Tuple2<String, Integer> value) throws Exception {
//                        return value.f0;
//                    }
//                })
//                .window(new WindowAssigner<Tuple2<String, Integer>, Window>() {
//                    @Override
//                    public Collection<Window> assignWindows(Tuple2<String, Integer> element, long timestamp, WindowAssignerContext context) {
//                        return null;
//                    }
//
//                    @Override
//                    public Trigger<Tuple2<String, Integer>, Window> getDefaultTrigger(StreamExecutionEnvironment env) {
//                        return null;
//                    }
//
//                    @Override
//                    public TypeSerializer<Window> getWindowSerializer(ExecutionConfig executionConfig) {
//                        return null;
//                    }
//
//                    @Override
//                    public boolean isEventTime() {
//                        return false;
//                    }
//                })
//                .sum(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result4 = env.addSource(new WordCountSource1ps())
                .flatMap(new FlatMapFunction1())
                .keyBy(0)
                // keyBy已经过时的方法
                .timeWindow(Time.seconds(30))
                // timeWindow已经过时的方法
                .sum(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result5 = env.addSource(new WordCountSource1ps())
                .flatMap(new FlatMapFunction1())
                .keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5))).sum(1);

        //result1.print();
        //result2.print();
        //result3.print();
        //result4.print();
        result5.print();
        env.execute("有这句才能执行任务，没有这句会Process finished with exit code 0直接结束");
    }

    public static class FlatMapFunction1 implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String cell : value.split("\\s+")) {
                out.collect(Tuple2.of(cell, 1));
            }
        }
    }

}
