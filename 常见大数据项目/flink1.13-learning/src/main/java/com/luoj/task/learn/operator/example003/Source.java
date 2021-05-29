package com.luoj.task.learn.operator.example003;


import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-29
 */
public class Source extends RichSourceFunction<Tuple2<Integer, Integer>>
        implements CheckpointedFunction {

    private final int totalRecords;
    private final int numberOfKeys;

    private volatile boolean running = true;
    private ListState<Integer> nextIndexState;
    private int nextRecord;

    public Source(int totalRecords, int numberOfKeys) {
        this.totalRecords = totalRecords;
        this.numberOfKeys = numberOfKeys;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext)
            throws Exception {
        this.nextIndexState =
                functionInitializationContext
                        .getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("next", Integer.class));

        if (nextIndexState.get().iterator().hasNext()) {
            nextRecord = nextIndexState.get().iterator().next();
        }
    }

    @Override
    public void run(SourceContext<Tuple2<Integer, Integer>> sourceContext) throws Exception {
        while (running && nextRecord < totalRecords) {
            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(
                        new Tuple2<>(nextRecord % numberOfKeys, nextRecord / numberOfKeys));
                nextRecord++;
            }

            Thread.sleep(2);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        nextIndexState.update(new ArrayList<>(Arrays.asList(nextRecord)));
    }
}