package com.luoj.task.learn.watermark.example001;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import static com.luoj.common.GenerateRandomDataUtils.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Random;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-01
 */
@Slf4j
public class GeneratingWatermarksExample001 {

    public static void main(String[] args) throws Exception {

        // 1. 初始化流式运行环境
        Configuration conf = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint");


        // 3. 定义数据源，每秒发送一个hadoop单词
        SingleOutputStreamOperator<Tuple6<String, String, String, String, Double, Long>> wordDSWithWaterMark = env.addSource(new RichSourceFunction<Tuple6<String, String, String, String, Double, Long>>() {

            private boolean isCanaled = false;

            @Override
            public void run(SourceContext<Tuple6<String, String, String, String, Double, Long>> ctx) throws Exception {
                while (!isCanaled) {
                    long  currentTimeStamp = System.currentTimeMillis();
                    ctx.collect(Tuple6.of(getRandomUserID(), getRandomUserName(), getRandomProductName(), getRandomProductID(), getRandomPriceName(), currentTimeStamp));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isCanaled = true;
            }
        })
//         .assignTimestampsAndWatermarks(
//                new WatermarkStrategy<Tuple6<String, String, String, String, Double, Long>>() {
//                    @Override
//                    public WatermarkGenerator<Tuple6<String, String, String, String, Double, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
//
//                        return null;
//                    }
//                }
//        );

//        .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)));

//        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple6<String, String, String, String, Double, Long>>() {
//            @Override
//            public long extractTimestamp(Tuple6<String, String, String, String, Double, Long> element) {
//                return element.f5;
//            }
//        });

        .assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple6<String, String, String, String, Double, Long>>() {
            @Override
            public WatermarkGenerator<Tuple6<String, String, String, String, Double, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return null;
            }
        });

        wordDSWithWaterMark.setParallelism(1).print();


        env.execute("GeneratingWatermarksExample001");

    }


}
