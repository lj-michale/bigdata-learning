package com.luoj.task.learn.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
/**
 * @author lj.michale
 * @description 对两条流监控，确定支付、收据两条数据是否能够匹配
 *              解决迟到数据，采用旁路侧输出
 * @date 2021-05-26
 */
public class KafkaFlinkTest12 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000);
        //Kafka源
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","114.116.219.197:5008");
        properties.setProperty("group.id","KafkaFlinkTest1a");

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>("flinktestkafka8", new SimpleStringSchema(), properties);
//        Map<KafkaTopicPartition,Long> specificStartOfssets = new HashMap<>();
//        specificStartOfssets.put(new KafkaTopicPartition("flinktestkafka4",0),0L);
//        specificStartOfssets.put(new KafkaTopicPartition("flinktestkafka4",1),0L);
//        specificStartOfssets.put(new KafkaTopicPartition("flinktestkafka4",2),0L);
//        flinkKafkaConsumer.setStartFromSpecificOffsets(specificStartOfssets);
        DataStreamSource<String> streamFromKafka = env.addSource(flinkKafkaConsumer);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<KafkaElement> mapresult = streamFromKafka.map(new MapFunction<String, KafkaElement>() {
            @Override
            public KafkaElement map(String value) throws Exception {
                String[] splits = value.split(",");
                System.out.println(Thread.currentThread()+"===haha"+value);
                return new KafkaElement(splits[0], splits[1], Math.toIntExact(Long.valueOf(splits[2])));
            }
        });

        SingleOutputStreamOperator<KafkaElement> watermarksstream = mapresult.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<KafkaElement>() {
            Long currentMaxTimestamp = 0L;
            Long maxOutOfOrderness = 5000L;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(KafkaElement element, long previousElementTimestamp) {
                long timestamp = element.getAge();
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;
            }
        });

        /**迟到数据标志*/
        OutputTag<KafkaElement> outputTag = new OutputTag<KafkaElement>("late-data"){};

        SingleOutputStreamOperator<KafkaOut> winstream = watermarksstream.keyBy("id")
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                //解决迟到数据
                .sideOutputLateData(outputTag)
                .apply(new WindowFunctionTest());

        /**
         * 迟到数据处理
         * 这里打印的迟到数据应该是没有经过window和apply处理的初始数据
         * */
        DataStream<KafkaElement> sideOutput = winstream.getSideOutput(outputTag);
        sideOutput.print();

        /**
         * 没有迟到数据处理
         * */
        winstream.map(new MapFunction<KafkaOut, Object>() {
            @Override
            public Object map(KafkaOut value) throws Exception {
                System.out.println(value);
                return null;
            }
        }).print();


        try {
            env.execute("this is kafkaflink job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 这是一个自定义的windown Function
    static class WindowFunctionTest implements WindowFunction<KafkaElement,KafkaOut, Tuple, TimeWindow>{
        /**
         * 用来触发窗口的计算
         * input作为所有这个窗口元素的迭代
         * */
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<KafkaElement> input, Collector<KafkaOut> out) throws Exception {
            System.out.println("string s是："+tuple.toString());
            Iterator<KafkaElement> iterators = input.iterator();
            StringBuilder stringBuilder = new StringBuilder();
            while(iterators.hasNext()){
                KafkaElement tmpkafkaElement = iterators.next();
                stringBuilder.append(tmpkafkaElement.getName());
            }
            out.collect(new KafkaOut(Integer.valueOf(tuple.getField(0)),new String(stringBuilder)));
        }
    }

    @NoArgsConstructor
    @Data
    static class KafkaOut {
       public KafkaOut(Integer valueOf, String s) { }
    }


}
