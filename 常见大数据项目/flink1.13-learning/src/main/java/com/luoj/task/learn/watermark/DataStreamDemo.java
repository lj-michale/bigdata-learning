package com.luoj.task.learn.watermark;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.StringUtils;

import java.util.Properties;

/**
 * @author lj.michale
 * @description
 * @date 2021-06-26
 */
public class DataStreamDemo {


    private static Properties properties;
    static {
        properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "event");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);
        // 每9秒发出一个watermark
        env.getConfig().setAutoWatermarkInterval(9000);
        DataStream<String> text = env.addSource(new FlinkKafkaConsumer("quick_app_msglifecycle", new SimpleStringSchema(), properties));

        DataStream<Tuple3<String, Long, Integer>> counts = text.filter(new FilterClass()).map(new LineSplitter())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>>() {
                    private long currentMaxTimestamp = 0L;
                    private final long maxOutOfOrderness = 10000L;   //这个控制失序已经延迟的度量
                    //获取EventTime
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Integer> element, long previousElementTimestamp) {
                        long timestamp = element.f1;
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        System.out.println(
                                "get timestamp is " + timestamp + " currentMaxTimestamp " + currentMaxTimestamp);
                        return timestamp;
                    }
                    //获取Watermark
                    @Override
                    public Watermark getCurrentWatermark() {
                        System.out.println("wall clock is " + System.currentTimeMillis() + " new watermark "
                                + (currentMaxTimestamp - maxOutOfOrderness));
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }
                }).keyBy(0).timeWindow(Time.seconds(20))
                // .allowedLateness(Time.seconds(10))
                .sum(2);

        counts.print();
        env.execute("Window WordCount");

    }

//    自定义获取timesStamp
//    private static class MyTimestamp extends AscendingTimestampExtractor<Tuple3<String, Long, Integer>> {
//        private static final long serialVersionUID = 1L;
//        public long extractAscendingTimestamp(Tuple3<String, Long, Integer> element) {
//            return element.f1;
//        }
//    }

    // 构造出element以及它的event time.然后把次数赋值为1
    public static final class LineSplitter implements MapFunction<String, Tuple3<String, Long, Integer>> {
        @Override
        public Tuple3<String, Long, Integer> map(String value) throws Exception {
            // TODO Auto-generated method stub
            String[] tokens = value.toLowerCase().split("\\W+");
            long eventtime = Long.parseLong(tokens[1]);
            return new Tuple3<String, Long, Integer>(tokens[0], eventtime, 1);
        }
    }

    // 过滤掉为null和whitespace的字符串
    public static final class FilterClass implements FilterFunction<String> {
        @Override
        public boolean filter(String value) throws Exception {
            if (StringUtils.isNullOrWhitespaceOnly(value)) {
                return false;
            } else {
                return true;
            }
        }

    }
}
