package com.luoj.task.example.cdc;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;

/**
 * @author lj.michale
 * @description mysql cdc demo
 * @date 2021-07-16
 */
public class MySqlBinlogSourceExample {

    public static void main(String[] args) throws Exception {

        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                // 获取两个数据库的所有表
                .databaseList("nacos", "aurora")
//                .tableList("user_log")
                .username("root")
                .password("abc1314520")
                // 自定义 解析器，讲数据解析成 json
                .deserializer(new CommonStringDebeziumDeserializationSchema("localhost", 3306))
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(sourceFunction)
            .map(str -> str)
            // 将数据发送到不同的 topic
            .addSink(new CommonKafkaSink())
            .setParallelism(1);

        env.execute();
    }
}