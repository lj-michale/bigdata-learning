package com.luoj.task.connector.clickhouse;


import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-07
 */
public class ClickHouseJDBCSinkFunction extends RichSinkFunction<Row> implements CheckpointedFunction {

    final ClickHouseJDBCOutputFormat outputFormat;

    public ClickHouseJDBCSinkFunction(ClickHouseJDBCOutputFormat outputFormat) {
        this.outputFormat = outputFormat;
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        outputFormat.writeRecord(value);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        outputFormat.snapshotStateFlush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext ctx = getRuntimeContext();
        outputFormat.setRuntimeContext(ctx);
        outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
    }

    @Override
    public void close() throws Exception {
        outputFormat.close();
        super.close();
    }
}
