package com.luoj.task.learn.alibaba.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能描述: 从Kafka读取消息数据，并在控制台进行打印。
 */
public class Kafka2Mysql {

    public static void main(String[] args) throws Exception {

        // Kafka {"msg": "welcome flink users..."}
        String sourceDDL = "CREATE TABLE kafka_source (\n" +
                " msg STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'mzpns',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset'\n" +
                ")";

        // Mysql
        String sinkDDL = "CREATE TABLE mysql_sink (\n" +
                " msg STRING \n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/bigdata?characterEncoding=utf-8&useSSL=false',\n" +
                "   'table-name' = 'cdn_log',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'abc1314520',\n" +
                "   'sink.buffer-flush.max-rows' = '1'\n" +
                ")";

        // 创建执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //注册source和sink
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        //数据提取
        Table sourceTab = tEnv.from("kafka_source");
        //这里我们暂时先使用 标注了 deprecated 的API, 因为新的异步提交测试有待改进...
        sourceTab.insertInto("mysql_sink");

        //执行作业
        tEnv.execute("Flink Hello World");

    }

}