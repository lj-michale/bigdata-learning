package com.luoj.task.learn.tablesql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-19
 */
public class TableFunctionExample001 {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

//        tEnv.registerFunction("split", new Split(" "));
//        tEnv.registerFunction("duplicator", new DuplicatorFunction());
//        tEnv.registerFunction("flatten", new FlattenFunction());

        tEnv.createTemporaryFunction("split", new Split(" "));
        tEnv.createTemporaryFunction("duplicator", new DuplicatorFunction());
        tEnv.createTemporaryFunction("flatten", new FlattenFunction());

        List<Tuple2<Long, String>> ordersData = new ArrayList<>();
        ordersData.add(Tuple2.of(2L, "Euro"));
        ordersData.add(Tuple2.of(1L, "US Dollar"));
        ordersData.add(Tuple2.of(50L, "Yen"));
        ordersData.add(Tuple2.of(3L, "Euro"));

        DataStream<Tuple2<Long,String>> ordersDataStream = env.fromCollection(ordersData);

        //  基于DataStream创建View
        tEnv.createTemporaryView("OrdersDataStream", ordersDataStream, $("amount"),$("currency"));
        Table orders = tEnv.from("OrdersDataStream");

        // tEnv.registerTable("Orders", orders);  // Flink1.11中写法
        // 将table注册成临时View
        tEnv.createTemporaryView("Orders", orders);

        // 查询 - left join
        Table result = tEnv.sqlQuery("SELECT o.currency, T.word, T.length FROM Orders as o LEFT JOIN LATERAL TABLE(split(currency)) as T(word, length) ON TRUE");
        tEnv.toAppendStream(result, Row.class).print();

        // join
        String sql = "SELECT o.currency, T.word, T.length FROM Orders as o ," +
                " LATERAL TABLE(split(currency)) as T(word, length)";

        // 多种类型参数
        String sql2 = "SELECT * FROM Orders as o , " +
                "LATERAL TABLE(duplicator(amount))," +
                "LATERAL TABLE(duplicator(currency))";

        // 不固定参数类型
        String sql3 = "SELECT * FROM Orders as o , " +
                "LATERAL TABLE(flatten(100,200,300))";


        env.execute("TableFunctionExample001");

    }

    /**
     * @descr 自定义函数-单个eval方法
     */
    public static class Split extends TableFunction<Tuple2<String,Integer>> {
        private String separator = ",";

        public Split(String separator) {
            this.separator = separator;
        }

        public void eval(String str) {
            for (String s : str.split(separator)) {
                collect(new Tuple2<String,Integer>(s, s.length()));
            }
        }
    }

    /**
     * @descr 自定义函数-多个eval方法
     * 注册多个eval方法，接收long或者string类型的参数，然后将他们转成string类型
     */
    public static class DuplicatorFunction extends TableFunction<String>{

        public void eval(Long i){
            eval(String.valueOf(i));
        }

        public void eval(String s){
            collect(s);
        }
    }

    /**
     * @descr 自定义函数-不固定参数
     * 接收不固定个数的int型参数,然后将所有数据依次返回
     */
    public static class FlattenFunction extends TableFunction<Integer>{
        public void eval(Integer... args){
            for (Integer i: args){
                collect(i);
            }
        }
    }

    /**
     * @descr 自定义函数-通过注解指定返回类型
     * 通过注册指定返回值类型，flink 1.11 版本开始支持
     */



}
