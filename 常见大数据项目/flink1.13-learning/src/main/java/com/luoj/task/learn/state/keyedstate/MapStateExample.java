package com.luoj.task.learn.state.keyedstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lj.michale
 * @description  keyed State使用案例
 * @date 2021-06-26
 */
public class MapStateExample {

    //统计每个用户每种行为的个数
    public static class UserBehaviorCnt extends RichFlatMapFunction<Tuple3<Long, String, String>, Tuple3<Long, String, Integer>> {

        //定义一个MapState句柄
        private transient MapState<String, Integer> behaviorCntState;

        // 初始化状态
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            MapStateDescriptor<String, Integer> userBehaviorMapStateDesc = new MapStateDescriptor<>(
                    "userBehavior",  // 状态描述符的名称
                    TypeInformation.of(new TypeHint<String>() {}),  // MapState状态的key的数据类型
                    TypeInformation.of(new TypeHint<Integer>() {})  // MapState状态的value的数据类型
            );
            behaviorCntState = getRuntimeContext().getMapState(userBehaviorMapStateDesc); // 获取状态
        }

        @Override
        public void flatMap(Tuple3<Long, String, String> value, Collector<Tuple3<Long, String, Integer>> out) throws Exception {
            Integer behaviorCnt = 1;
            // 如果当前状态包括该行为，则+1
            if (behaviorCntState.contains(value.f1)) {
                behaviorCnt = behaviorCntState.get(value.f1) + 1;
            }
            // 更新状态
            behaviorCntState.put(value.f1, behaviorCnt);
            out.collect(Tuple3.of(value.f0, value.f1, behaviorCnt));
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        // 模拟数据源[userId,behavior,product]
        DataStreamSource<Tuple3<Long, String, String>> userBehaviors = env.fromElements(
                Tuple3.of(1L, "buy", "iphone"),
                Tuple3.of(1L, "cart", "huawei"),
                Tuple3.of(1L, "buy", "logi"),
                Tuple3.of(1L, "fav", "oppo"),
                Tuple3.of(2L, "buy", "huawei"),
                Tuple3.of(2L, "buy", "onemore"),
                Tuple3.of(2L, "fav", "iphone"));
        userBehaviors
                .keyBy(0)
                .flatMap(new UserBehaviorCnt())
                .print();

        env.execute("MapStateExample");

    }
}

