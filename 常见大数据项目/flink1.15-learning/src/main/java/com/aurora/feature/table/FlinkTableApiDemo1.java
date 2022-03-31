package com.aurora.feature.table;

import com.aurora.generate.WordCountSource1ps;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.interpreter.Row;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;//可以使用$("变量名")

/**
 * @descri 使用TableAPI实现流批一体
 *
 * @author lj.michale
 * @date 2022-04-01
 */
@Slf4j
public class FlinkTableApiDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Flink1.14不需要设置Planner
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                // .useBlinkPlanner()已过期
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<String> data = env.addSource(new WordCountSource1ps());

        // 过时
        Table table1 = tableEnv.fromDataStream(data, "word");
        Table table1_1 = table1.where($("word").like("%5%"));
        // 过时
        System.out.println("tableEnv.explain(table1_1) = " + tableEnv.explain(table1_1));
        // 过时
        tableEnv.toAppendStream(table1_1, Row.class).print("table1_1");
        System.out.println("env.getExecutionPlan() = " + env.getExecutionPlan());

        env.execute();

    }

}
