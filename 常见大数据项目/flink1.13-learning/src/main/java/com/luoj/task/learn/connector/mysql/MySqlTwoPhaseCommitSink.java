package com.luoj.task.learn.connector.mysql;


import com.alibaba.fastjson.JSONObject;
import com.luoj.common.DBConnectUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author lj.michale
 * @description
 * 自定义kafka to mysql,继承TwoPhaseCommitSinkFunction,实现两阶段提交
 * @date 2021-05-27
 */
public class MySqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<ObjectNode, Connection, Void> {

    private static final Logger log = LoggerFactory.getLogger(MySqlTwoPhaseCommitSink.class);

    public MySqlTwoPhaseCommitSink(){
        super(new KryoSerializer<>(Connection.class,new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    /**
     * 执行数据库入库操作  task初始化的时候调用
     * @param connection
     * @param objectNode
     * @param context
     * @throws Exception
     */
    @Override
    protected void invoke(Connection connection, ObjectNode objectNode, Context context) throws Exception {
        log.info("start invoke...");
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        log.info("===>date:" + date + " " + objectNode);
        log.info("===>date:{} --{}",date,objectNode);
        String value = objectNode.get("value").toString();
        log.info("objectNode-value:" + value);
        JSONObject valueJson = JSONObject.parseObject(value);
        String value_str = (String) valueJson.get("value");
        String sql = "insert into `mysqlExactlyOnce_test` (`value`,`insert_time`) values (?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1,value_str);
        Timestamp value_time = new Timestamp(System.currentTimeMillis());
        ps.setTimestamp(2,value_time);
        log.info("要插入的数据:{}--{}",value_str,value_time);
        //执行insert语句
        ps.execute();
        //手动制造异常
        if(Integer.parseInt(value_str) == 15) {
            System.out.println(1 / 0);
        }
    }

    /**
     * 获取连接，开启手动提交事物（getConnection方法中）
     * @return
     * @throws Exception
     */
    @Override
    protected Connection beginTransaction() throws Exception {
        log.info("start beginTransaction.......");
        String url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
        Connection connection = DBConnectUtil.getConnection(url, "root", "123456");
        return connection;
    }

    /**
     *预提交，这里预提交的逻辑在invoke方法中
     * @param connection
     * @throws Exception
     */
    @Override
    protected void preCommit(Connection connection) throws Exception {
        log.info("start preCommit...");
    }

    /**
     * 如果invoke方法执行正常，则提交事务
     * @param connection
     */
    @Override
    protected void commit(Connection connection) {
        log.info("start commit...");
        DBConnectUtil.commit(connection);
    }

    /**
     * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
     * @param connection
     */
    @Override
    protected void abort(Connection connection) {
        log.info("start abort rollback...");
        DBConnectUtil.rollback(connection);
    }
}
