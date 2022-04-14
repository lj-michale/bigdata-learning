package com.aurora.example.example03;

import com.aurora.feature.window.MyEventTimeWindow;
import com.aurora.generate.SentenceUDSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @descri 自定义窗口使用示例
 *
 * @author lj.michale
 * @date 2022-04-14
 */
@Slf4j
public class MyEventTimeWindowByFlink {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.addSource(new SentenceUDSource());

        // 扁平化处理
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMapDs = dataStreamSource
                .flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (value, out) -> {
            Arrays.stream(value.split(" ")).forEach(s -> out.collect(Tuple2.of(s, 1L)));
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 调用自定义窗口，进行窗口计算， WindowAll： DataStream → AllWindowedStream
        AllWindowedStream allWindowedStream  = flatMapDs.windowAll(MyEventTimeWindow.of(Time.days(1), Time.hours(1)));

        /**
         * Window Apply:
         * WindowedStream → DataStream
         * AllWindowedStream → DataStream
         *
         * 注意：如果你使用的是 windowAll 算子，则需要使用 AllWindowFunction 方法
         */
        // applying an AllWindowFunction on non-keyed window stream
        SingleOutputStreamOperator reOutputStream =  allWindowedStream.apply (new AllWindowFunction<Tuple2<String, Long>, Integer, Window>() {
            @Override
            public void apply(Window window, Iterable<Tuple2<String, Long>> iterable, Collector<Integer> collector) throws Exception {
                int sum = 0;
                while (iterable.iterator().hasNext()) {
                    Tuple2<String, Long> tuple2 = iterable.iterator().next();
                    sum += tuple2.f1;
                }
                collector.collect (new Integer(sum));
            }
        });

        reOutputStream.print();

        env.execute("MyEventTimeWindowByFlink");

    }

}
