package com.luoj.task.learn.dataset;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lj.michale
 * @description DataStream Api实现WordCount
 *              Flink1.12中DataStream支持流处理也支持批处理
 * @date 2021-04-19
 */
public class WordCount2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);  // 注意使用DataStream实现批处理
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING); // 注意使用DataStream实现流处理
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC); // 注意使用DataStream自动选择使用批处理还是流处理

        DataStreamSource<String> lines = env.fromElements("itcast hadoop spark","itcast hadoop spark","itcast hadoop","itcast");

        ////////////////////////// Transformation Start ///////////////////////////////////////
        DataStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] arr = s.split(" ");
                for (String word: arr) {
                    collector.collect(word);
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> grouped = wordAndOne.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = grouped.sum(1);

        ////////////////////////// Transformation End ///////////////////////////////////////

        result.print();

    }

}
