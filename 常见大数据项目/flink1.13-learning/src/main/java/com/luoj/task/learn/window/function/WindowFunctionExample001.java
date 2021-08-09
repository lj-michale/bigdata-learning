package com.luoj.task.learn.window.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-09
 */
@Slf4j
public class WindowFunctionExample001 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);

        DataStream<String> dataStream = env.socketTextStream("192.168.200.58",7777);

        // 滑动窗口
        dataStream.map(i -> Integer.valueOf(i)).keyBy(new KeySelector<Integer, Object>() {
            @Override
            public Object getKey(Integer value) throws Exception {
                return 0;
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//        .reduce(new ReduceFunction<Integer>() {
//            // 比较两个数大小等 可以实现 max min sum 等操作
//            @Override
//            public Integer reduce(Integer value1, Integer value2) throws Exception {
//                return null;
//            }
//        });
        // 增量聚合函数: ResuceFunction、AggregateFunction
        // 实现一个计数器，统计有多少个数据
        .aggregate(new AggregateFunction<Integer, Integer, Integer>() {
            // 创建一个累加器，初始值
            @Override
            public Integer createAccumulator() {
                return 0;
            }

            // 累加规则
            @Override
            public Integer add(Integer value, Integer accumulator) {
                return accumulator + 1;
            }

            // 输出结果
            @Override
            public Integer getResult(Integer accumulator) {
                return accumulator;
            }

            // merge一般使用的是 session window 做合并操作
            @Override
            public Integer merge(Integer a, Integer b) {
                return null;
            }
        }).print();

        // 滑动窗口
        dataStream.map(i -> Integer.valueOf(i)).keyBy(new KeySelector<Integer, Object>() {
            @Override
            public Object getKey(Integer value) throws Exception {
                return 0;
            }
            // 攒齐5s 的数据，求平均值
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        // 参数1 输入类型，2 输出类型，3 key的类型，4 就是个TimeWindow
        // 全窗口函数: ProcessWindowFunction、WindowFunction
        .apply(new WindowFunction<Integer, Integer, Object, TimeWindow>() {
            // 参数1 key的类型，2 就是window，3 所有输入的数据，4 输出收集器
            @Override
            public void apply(Object o, TimeWindow window, Iterable<Integer> input, Collector<Integer> out) throws Exception {
                List<Integer> list = IteratorUtils.toList(input.iterator());
                // 求个数
                int count = list.size();
                // 求和
                int sum = list.stream().reduce((i,y) -> i+y).get();
                // 求均值
                int avg = sum/count;
                out.collect(avg);
            }
        }).print();

        env.execute("WindowFunctionExample001");

    }

}
