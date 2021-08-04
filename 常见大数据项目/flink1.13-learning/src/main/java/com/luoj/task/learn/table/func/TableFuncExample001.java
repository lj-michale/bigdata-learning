package com.luoj.task.learn.table.func;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-04
 */
@Slf4j
public class TableFuncExample001 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        List<Tuple2<Long,String>> ordersData = new ArrayList<>();
        ordersData.add(Tuple2.of(2L, "Euro"));
        ordersData.add(Tuple2.of(1L, "US Dollar"));
        ordersData.add(Tuple2.of(50L, "Yen"));
        ordersData.add(Tuple2.of(3L, "Euro"));

        // 注册函数
        tEnv.registerFunction("split", new SplitFunc.Split(" "));
        tEnv.registerFunction("duplicator", new DuplicatorFunction());
        tEnv.registerFunction("flatten", new SplitFunc.FlattenFunction());


        DataStream<Tuple2<Long,String>> ordersDataStream = env.fromCollection(ordersData);
        Table orders = tEnv.fromDataStream(ordersDataStream, "amount, currency, proctime.proctime");
        tEnv.registerTable("Orders", orders);

        ////////////  查询
        ////////////  left join
        Table result = tEnv.sqlQuery(
                "SELECT o.currency, T.word, T.length " +
                "FROM Orders as o LEFT JOIN LATERAL TABLE(split(currency)) as T(word, length) ON TRUE");
        tEnv.toAppendStream(result, Row.class).print();

        ////////////  join
        String sql = "SELECT o.currency, T.word, T.length FROM Orders as o ," +
                " LATERAL TABLE(split(currency)) as T(word, length)";
        Table result1 = tEnv.sqlQuery(sql);
        tEnv.toAppendStream(result1, Row.class).print();


        ////////////  多种类型参数
//        String sql2 = "SELECT * FROM Orders as o , " +
//                "LATERAL TABLE(duplicator(amount))," +
//                "LATERAL TABLE(duplicator(currency))";
//        TableResult result2 = tEnv.executeSql(sql2);
//        tEnv.toAppendStream((Table) result2, Row.class).print();

        ////////////  不固定参数类型
        String sql3 = "SELECT * FROM Orders as o , " +
                "LATERAL TABLE(flatten(100,200,300))";
        Table result3 = tEnv.sqlQuery(sql3);
        tEnv.toAppendStream(result3, Row.class).print();

        env.execute("TableFuncExample001");

    }

}
