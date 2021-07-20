package com.luoj.task.learn.sink.example001;

import java.util.function.Function;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat.RecordExtractor;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Preconditions;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-02
 */
public class JcacheSink {

    public static <T> SinkFunction<T> sink(String sql, JdbcStatementBuilder<T> statementBuilder, JdbcConnectionOptions connectionOptions) {
        return sink(sql, statementBuilder, JdbcExecutionOptions.defaults(), connectionOptions);
    }

    public static <T> SinkFunction<T> sink(String sql, JdbcStatementBuilder<T> statementBuilder, JdbcExecutionOptions executionOptions, JdbcConnectionOptions connectionOptions) {
        return new GenericJdbcSinkFunction(new JdbcBatchingOutputFormat(new SimpleJdbcConnectionProvider(connectionOptions), executionOptions, (context) -> {
            Preconditions.checkState(!context.getExecutionConfig().isObjectReuseEnabled(), "objects can not be reused with JDBC sink function");
            return JdbcBatchStatementExecutor.simple(sql, statementBuilder, Function.identity());
        }, RecordExtractor.identity()));
    }

    private JcacheSink() {
    }

}
