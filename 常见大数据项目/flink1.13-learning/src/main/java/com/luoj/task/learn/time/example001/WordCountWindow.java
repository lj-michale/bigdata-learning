package com.luoj.task.learn.time.example001;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Iterator;

/**
 * @author lj.michale
 * @description Flink时间机制相关知识整理
 * @date 2021-07-01
 */
@Slf4j
public class WordCountWindow {


    public static void main(String[] args) throws Exception {

        // 1. 初始化流式运行环境
        Configuration conf = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint");

        // 2. 设置时间处理类型，这里设置的方式处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 3. 定义数据源，每秒发送一个hadoop单词
        SingleOutputStreamOperator<Tuple2<String, Long>> wordDSWithWaterMark = env.addSource(new RichSourceFunction<Tuple2<String, Long>>() {

            private boolean isCanaled = false;
            private int TOTAL_NUM = 20;

            @Override
            public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                while (!isCanaled) {
                    ctx.collect(Tuple2.of("hadooop", System.currentTimeMillis()));

                    // 打印窗口开始、结束时间
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    System.out.println("事件发送时间:" + sdf.format(System.currentTimeMillis()));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isCanaled = true;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(Tuple2<String, Long> element) {
                return element.f1;
            }
        });

        // 4. 每5秒进行一次，分组统计
        // 4.1 转换为元组
        wordDSWithWaterMark.map(word -> {
            return Tuple2.of(word.f0, 1);

        })
        // 指定返回类型
        .returns(Types.TUPLE(Types.STRING, Types.INT))
        // 按照单词进行分组
        .keyBy(t -> t.f0)
        // 滚动窗口，3秒计算一次
        .timeWindow(Time.seconds(3))
        .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        }, new RichWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
            @Override
            public void apply(String word, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {

                // 打印窗口开始、结束时间
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                System.out.println("窗口开始时间:" + sdf.format(window.getStart())
                        + " 窗口结束时间：" + sdf.format(window.getEnd())
                        + " 窗口计算时间:" + sdf.format(System.currentTimeMillis()));

                int sum = 0;
                Iterator<Tuple2<String, Integer>> iterator = input.iterator();
                while(iterator.hasNext()) {
                    Integer count = iterator.next().f1;
                    sum += count;
                }
                out.collect(Tuple2.of(word, sum));
            }
        }).print();

        env.execute("app");
    }



}
