package com.aurora.example;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import static org.apache.flink.table.api.Expressions.$;

/**
 * @descri 使用TableAPI实现流批一体
 *
 * @author lj.michale
 * @date 2022-05-01
 */
public class FlinkTableApiDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> data = env.addSource(new WordCountSource1ps());

        // 过时
        Table table1 = tableEnv.fromDataStream(data, "word");
        Table table1_1 = table1.where($("word").like("%5%"));
        // 过时
        tableEnv.toAppendStream(table1_1, Row.class).print("table1_1");
        System.out.println("env.getExecutionPlan() = " + env.getExecutionPlan());

        env.execute();

    }

}
