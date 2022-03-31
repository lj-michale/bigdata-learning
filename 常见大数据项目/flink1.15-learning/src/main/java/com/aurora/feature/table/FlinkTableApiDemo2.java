package com.aurora.feature.table;

import com.aurora.generate.WordCountSource1ps;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;//可以使用$("变量名")

/**
 * @descri 使用TableAPI实现流批一体
 *
 * @author lj.michale
 * @date 2022-04-01
 */
public class FlinkTableApiDemo2 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Flink1.14不需要设置Planner
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<String> data = env.addSource(new WordCountSource1ps());

        System.out.println("***********新方法**************");

        ArrayList<String> strings = new ArrayList<>();
        // 必须写f0
        strings.add("f0");
        List<DataType> dataTypes = new ArrayList<DataType>();
        dataTypes.add(DataTypes.STRING());
        Schema schema = Schema.newBuilder()
                .fromFields(strings, dataTypes)
                .build();
        List<Schema.UnresolvedColumn> columns = schema.getColumns();

        for (Schema.UnresolvedColumn column : columns) {
            System.out.println("column = " + column);
        }

        Table table2 = tableEnv.fromDataStream(data, schema);
        // 必须写f0
        Table table2_1 = table2.where($("f0").like("%5%"));
        System.out.println("table2_1.explain() = " + table2_1.explain(ExplainDetail.JSON_EXECUTION_PLAN));
        tableEnv.toDataStream(table2_1, Row.class).print("table2_1");

        System.out.println("env.getExecutionPlan() = " + env.getExecutionPlan());
        env.execute();

    }

}
