package com.luoj.task.learn.datastreamapi;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

/** https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/datastream_execution_mode.html
 *  https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/event_timestamps_watermarks.html
 * @author lj.michale
 * @description
 * @date 2021-04-12
 */
public class DataStreamAPIByJava {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        DataStreamSource<String> source = env.fromElements(...);
//        source.name("source")
//                .map(...).name("map1")
//                .map(...).name("map2")
//                .rebalance()
//                .map(...).name("map3")
//                .map(...).name("map4")
//                .keyBy((value) -> value)
//                .map(...).name("map5")
//                .map(...).name("map6")
//                .sinkTo(...).name("sink");

//        DataStream<MyEvent> stream = env.addSource(new FlinkKafkaConsumer<MyEvent>(topic, schema, props));
//        stream.keyBy( (event) -> event.getUser() )
//                .window(TumblingEventTimeWindows.of(Time.hours(1)))
//                .reduce( (a, b) -> a.add(b) )
//                .addSink(...);

        //如上所述，通常情况下，你不用实现此接口，而是可以使用 WatermarkStrategy 工具类中通用的 watermark 策略，或者可以使用这个工具类将自定义的 TimestampAssigner 与 WatermarkGenerator 进行绑定。
        // 例如，你想要要使用有界无序（bounded-out-of-orderness）watermark 生成器和一个 lambda 表达式作为时间戳分配器，那么可以按照如下方式实现：
//        WatermarkStrategy
//                .<Tuple2<Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
//                .withTimestampAssigner((event, timestamp) -> event.f0);



    }

}
