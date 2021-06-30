package com.luoj.task.learn.exactlyonce;

import com.luoj.common.DruidConnectionUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author lj.michale
 * @description
 * @date 2021-06-30
 */
public class OracleTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<String, Connection, Void> {

    private static final Logger logger = LogManager.getLogger(OracleTwoPhaseCommitSink.class);

    public OracleTwoPhaseCommitSink(TypeSerializer<Connection> transactionSerializer, TypeSerializer<Void> contextSerializer) {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    /**
     * 获取连接，开启手动提交事物（getConnection方法中）
     * @return 数据库连接
     * @throws Exception exception
     */
    @Override
    protected Connection beginTransaction() throws Exception {
        Connection connection = DruidConnectionUtils.getConnection();
        connection.setAutoCommit(false);
        return connection;
    }

    /**
     * 执行数据入库操作
     * @param connection 连接
     * @param sql 执行SQL
     * @param context context
     */
    @Override
    protected void invoke(Connection connection, String sql, Context context) throws Exception {
        try {
            if (!"".equals(sql)) {
                PreparedStatement statement = connection.prepareStatement(sql);
                statement.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 预提交，这里预提交的逻辑在invoke方法中
     * @param connection ConnectionState
     */
    @Override
    protected void preCommit(Connection connection) throws Exception {

    }

    /**
     * 如果invoke执行正常则提交事物
     * @param connection ConnectionState
     */
    @Override
    protected void commit(Connection connection) {
        try {
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException("提交事物异常");
        }
    }

    /**
     * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
     * @param connection Connection
     */
    @Override
    protected void abort(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException e) {
            throw new RuntimeException("回滚事物异常");
        }
    }

}
