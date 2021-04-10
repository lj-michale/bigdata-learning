package com.luoj.task.learn.tablesql;

import com.luoj.task.learn.source.Source_Demo05_Customer_MySQL;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;

/**
 * @author lj.michale
 * @description
 * @date 2021-04-10
 */
public class FlinkTableSQL {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings mySetting = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, mySetting);

        DataStreamSource<String> dataStream = env.readTextFile("");
        SingleOutputStreamOperator<Source_Demo05_Customer_MySQL.Student> inputData = dataStream.map(new MapFunction<String, Source_Demo05_Customer_MySQL.Student>() {
            @Override
            public Source_Demo05_Customer_MySQL.Student map(String s) throws Exception {
                String[] splits = s.split(",");
                return new Source_Demo05_Customer_MySQL.Student(splits[0], Integer.parseInt(splits[1]));
            }
        });

        // DataSet -> Table
        Table table  = tableEnv.fromDataStream(inputData);

        // 注册为student表
        tableEnv.createTemporaryView("student", table);

        // SQL Query
        Table sqlQuery = tableEnv.sqlQuery("select count(1), avg(age) from student");


        // CsvTableSink
        CsvTableSink csvTableSink = new CsvTableSink("", ",", 1, FileSystem.WriteMode.OVERWRITE);

        // 注册TableSink
       sqlQuery.executeInsert("outputTable");

        env.execute("Start Table Api for Stream");

    }
}
