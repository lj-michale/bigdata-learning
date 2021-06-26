package com.luoj.task.learn.state.operatorstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;

/**
 * @author lj.michale
 * @description
 * @date 2021-06-26
 */
public class ListCheckpointedExample {

    private static class UserBehaviorCnt
            extends RichFlatMapFunction<Tuple3<Long, String, String>, Tuple2<String, Long>>
            implements ListCheckpointed<Long> {
        private Long userBuyBehaviorCnt = 0L;
        @Override
        public void flatMap(Tuple3<Long, String, String> value, Collector<Tuple2<String, Long>> out) throws Exception {
            if(value.f1.equals("buy")){
                userBuyBehaviorCnt ++;
                out.collect(Tuple2.of("buy",userBuyBehaviorCnt));
            }
        }
        @Override
        public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
            //返回单个元素的List集合，该集合元素是用户购买行为的数量
            return Collections.singletonList(userBuyBehaviorCnt);
        }
        @Override
        public void restoreState(List<Long> state) throws Exception {
            // 在进行扩缩容之后，进行状态恢复，需要把其他subtask的状态加在一起
            for (Long cnt : state) {
                userBuyBehaviorCnt += 1;
            }
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

        userBehaviors.flatMap(new UserBehaviorCnt()).print();

        env.execute("ListCheckpointedExample");
    }
}

