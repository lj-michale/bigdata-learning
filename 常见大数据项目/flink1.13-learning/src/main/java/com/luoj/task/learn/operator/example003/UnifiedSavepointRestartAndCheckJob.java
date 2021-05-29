package com.luoj.task.learn.operator.example003;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-29
 */
public class UnifiedSavepointRestartAndCheckJob {

    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);

        int totalRecords = tool.getInt("total_records");
        int numberOfKeys = tool.getInt("num_keys");
        int parallelism = tool.getInt("parallelism");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(20, CheckpointingMode.EXACTLY_ONCE);

        String stateBackendName = tool.get("state_backend");
        switch (stateBackendName) {
            case "hashmap":
                env.setStateBackend(new HashMapStateBackend());
                break;
            case "rocksdb":
                env.setStateBackend(new EmbeddedRocksDBStateBackend());
                break;
            case "rocksdb_incre":
                env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
                break;
            default:
                throw new RuntimeException("Not supported statebackend " + stateBackendName);
        }

        String stateBackendPath = tool.get("state_backend_path");
        env.getCheckpointConfig().setCheckpointStorage(stateBackendPath);

        KeySelector<Tuple2<Integer, Integer>, Integer> keySelector = tuple -> tuple.f0;

        DataStream<Tuple2<Integer, Integer>> source =
                env.addSource(new Source(totalRecords, numberOfKeys)).uid("source");

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> valueStateStream =
                source.keyBy(keySelector)
                        .transform(
                                "value state",
                                source.getType(),
                                new ValueStateOperator(totalRecords))
                        .uid("value_state");

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> reducingStateStream =
                valueStateStream
                        .keyBy(keySelector)
                        .transform(
                                "reducing state",
                                source.getType(),
                                new ReduceStateOperator(totalRecords))
                        .uid("reducing_state");

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> aggregatingStateStream =
                reducingStateStream
                        .keyBy(keySelector)
                        .transform(
                                "aggregating state",
                                source.getType(),
                                new AggregationStateOperator(totalRecords))
                        .uid("aggregating_state");

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> listStateStream =
                aggregatingStateStream
                        .keyBy(keySelector)
                        .transform(
                                "list state", source.getType(), new ListStateOperator(totalRecords))
                        .uid("list_state");

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> mapStateStream =
                listStateStream
                        .keyBy(keySelector)
                        .transform(
                                "map state", source.getType(), new MapStateOperator(totalRecords))
                        .uid("map_state");

        String resultPath = tool.get("result_path");
        // Add the final check sink
        valueStateStream
                .getSideOutput(ValueStateOperator.STATE_RESULT_TAG)
                .addSink(new CollectResultSink(resultPath + "/value_state"))
                .setParallelism(1);
        reducingStateStream
                .getSideOutput(ReduceStateOperator.STATE_RESULT_TAG)
                .addSink(new CollectResultSink(resultPath + "/reducing_state"))
                .setParallelism(1);
        aggregatingStateStream
                .getSideOutput(AggregationStateOperator.STATE_RESULT_TAG)
                .addSink(new CollectResultSink(resultPath + "/aggregating_state"))
                .setParallelism(1);
        listStateStream
                .getSideOutput(AggregationStateOperator.STATE_RESULT_TAG)
                .addSink(new CollectResultSink(resultPath + "/list_state"))
                .setParallelism(1);
        mapStateStream
                .getSideOutput(AggregationStateOperator.STATE_RESULT_TAG)
                .addSink(new CollectResultSink(resultPath + "/map_state"))
                .setParallelism(1);

        env.execute();

        ValueStateOperator.checkResult(resultPath + "/value_state", totalRecords, numberOfKeys);
        ReduceStateOperator.checkResult(resultPath + "/reducing_state", totalRecords, numberOfKeys);
        AggregationStateOperator.checkResult(
                resultPath + "/aggregating_state", totalRecords, numberOfKeys);
        ListStateOperator.checkResult(resultPath + "/list_state", totalRecords, numberOfKeys);
        MapStateOperator.checkResult(resultPath + "/map_state", totalRecords, numberOfKeys);
    }
}
