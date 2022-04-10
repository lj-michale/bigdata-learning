package com.aurora.feature.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-04-10
 */
public class FlinkStreamingDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置任务的最大并行度 也就是keyGroup的个数
        env.setMaxParallelism(128);
        //env.getConfig().setAutoWatermarkInterval(1000L);
        // 设置开启checkpoint
        env.enableCheckpointing(10000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.15-learning\\checkpoint");

        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<Jason> dataStreamSource = env.addSource(new UserDefinedSource());
        dataStreamSource.keyBy(k -> k.getName())
                .process(new KeyedProcessFunction<String, Jason, Jason>() {
                    private ValueState<Integer> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("state", Types.INT);
                        state = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void processElement(Jason value, KeyedProcessFunction<String, Jason, Jason>.Context ctx, Collector<Jason> out) throws Exception {
                        if (state.value() != null) {
                            System.out.println("状态里面有数据 :" + state.value());
                            value.setAge(state.value() + value.getAge());
                            state.update(state.value() + value.getAge());
                        } else {
                            state.update(value.getAge());
                        }
                        out.collect(value);
                    }
                }).uid("my-uid")
                .print("local-print");

        env.execute();
    }
}