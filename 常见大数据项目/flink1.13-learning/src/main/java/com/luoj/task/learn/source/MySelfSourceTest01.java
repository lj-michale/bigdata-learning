package com.luoj.task.learn.source;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.util.Random;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-09
 */
public class MySelfSourceTest01 {

    static Logger logger = Logger.getLogger(MySelfSourceTest01.class);

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, String>> redisDS = env.addSource(new SourceFunction<Tuple2<String, String>>() {
            @Override
            public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
                Random random = new Random();
                while (true) {
                    ctx.collect(new Tuple2<String, String>( "1", "A"));
                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() {
            }
        });

        DataStreamSource<Tuple2<String, String>> redisDSB = env.addSource(new SourceFunction<Tuple2<String, String>>() {
            @Override
            public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
                while (true) {
                    ctx.collect(new Tuple2<String, String>("2", "B"));
                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() {

            }
        });

        DataStreamSource<Tuple2<String, String>> redisDSC = env.addSource(new SourceFunction<Tuple2<String, String>>() {
            @Override
            public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
                while (true) {
                    ctx.collect(new Tuple2<String, String>("3", "C"));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        DataStream<Tuple2<String, String>> union = redisDS.union(redisDSB).union(redisDSC);

        KeyedStream<Tuple2<String, String>, Integer> tuple2IntegerKeyedStream = union.keyBy(new KeySelector<Tuple2<String, String>, Integer>() {
            @Override
            public Integer getKey(Tuple2<String, String> value) throws Exception {
                return Integer.parseInt(value.f0) % 1;
            }
        });

        tuple2IntegerKeyedStream.timeWindow(Time.seconds(2)).apply(new WindowFunction<Tuple2<String,String>, String, Integer, TimeWindow>() {
            @Override
            public void apply(Integer integer, TimeWindow window, Iterable<Tuple2<String, String>> input, Collector<String> out) throws Exception {
                StringBuffer stringBuffer = new StringBuffer();
                input.forEach(t -> {
                    stringBuffer.append(t.toString()).append("  ");
                });
                System.out.println(stringBuffer.toString());
                System.out.println();
            }
        });

//                .timeWindow(Time.seconds(2)).apply(new WindowFunction<Tuple2<String,String>, Tuple2<String,String>, Tuple, TimeWindow>() {
//            @Override
//            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, String>> input, Collector<Tuple2<String, String>> out) throws Exception {
//                StringBuffer stringBuffer = new StringBuffer();
//                input.forEach(t -> {
//                    stringBuffer.append(t.toString()).append("  ");
//                });
//                System.out.println(stringBuffer.toString());
//                System.out.println();
//            }
//        });
//        union.timeWindowAll(Time.seconds(2)).apply(new AllWindowFunction<Tuple2<String,String>, String, TimeWindow>() {
//            @Override
//            public void apply(TimeWindow window, Iterable<Tuple2<String, String>> values, Collector<String> out) throws Exception {
//                StringBuffer stringBuffer = new StringBuffer();
//                values.forEach(t -> {
//                    stringBuffer.append(t.toString()).append("  ");
//                });
//                System.out.println(stringBuffer.toString());
//                System.out.println();
//            }
//        });

        try {
            env.execute("a");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}