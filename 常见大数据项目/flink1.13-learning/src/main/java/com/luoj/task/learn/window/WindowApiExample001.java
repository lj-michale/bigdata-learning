package com.luoj.task.learn.window;

import com.luoj.bean.MarketingUserBehavior;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-09
 */
@Slf4j
public class WindowApiExample001 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> dataStream = env.fromElements(10,27,3,7,1,50,22,19);
        // 滚动窗口，可以看这个方法的实现类
        dataStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(15)));
        // session 窗口
        dataStream.keyBy(new KeySelector<Integer, String>() {
            @Override
            public String getKey(Integer value) throws Exception {
                String key = value.toString().substring(0, 1);
                return key;
            }
        }).window(EventTimeSessionWindows.withGap(Time.minutes(1)));
        // 滚动窗口
        dataStream.keyBy(0).timeWindow(Time.seconds(15));
        // 滑动窗口
        dataStream.keyBy(0).timeWindow(Time.seconds(20),Time.seconds(5));
        // 滑动窗口
        dataStream.keyBy(0).window(SlidingProcessingTimeWindows.of(Time.seconds(20),Time.seconds(5)));
        // 滚动计数窗口
        dataStream.keyBy(0).countWindow(10);
        // 滑动计数窗口
        dataStream.keyBy(0).countWindow(10,3);

        DataStream<Tuple2<String,Integer>> ds = env.fromElements(Tuple2.of("tom", 12),Tuple2.of("jack", 20)
                ,Tuple2.of("rose", 18),Tuple2.of("tom",20),Tuple2.of("jack", 32)
                ,Tuple2.of("rose", 11),Tuple2.of("frank", 12),Tuple2.of("putin", 67)
                ,Tuple2.of("frank", 22),Tuple2.of("tom", 18),Tuple2.of("putin", 99)
                ,Tuple2.of("jack", 28),Tuple2.of("rose", 19),Tuple2.of("putin", 33)
                ,Tuple2.of("rose", 88),Tuple2.of("jack", 47),Tuple2.of("tom", 66));

        KeyedStream<Tuple2<String,Integer>, String> ks = ds.keyBy(new KeySelector<Tuple2<String,Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                String key =value.f0;
                return key;
            }
        });

        System.out.println(Tuple2.of("rose", 18).f0.hashCode());
        System.out.println(Tuple2.of("frank", 12).f0.hashCode());
        //ks.print();
        DataStream<Tuple2<String, Integer>> st =ks.max(1);
        st.print();

        env.execute("WindowApiExample001");

    }

    // 自定义 KeySelector
//    public static class MyKeySelector implements KeySelector<MarketingUserBehavior, Tuple2<String,String>> {
//        @Override
//        public Tuple2<String, String> getKey(MarketingUserBehavior marketingUserBehavior) throws Exception {
//            return new Tuple2<>(marketingUserBehavior.getChannel(), marketingUserBehavior.getBehavior());
//        }
//    }


}
