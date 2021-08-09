package com.luoj.task.learn.window.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
//               .reduce(new ReduceFunction<Integer>() {
//                   // 比较两个数大小等 可以实现 max min sum 等操作
//                   @Override
//                   public Integer reduce(Integer value1, Integer value2) throws Exception {
//                       return null;
//                   }
//               });
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

        env.execute("WindowFunctionExample001");
    }

}
