package com.luoj.task.learn.window;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
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
        dataStream.keyBy(0).window(EventTimeSessionWindows.withGap(Time.minutes(1)));
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

        env.execute("WindowApiExample001");

    }


}
