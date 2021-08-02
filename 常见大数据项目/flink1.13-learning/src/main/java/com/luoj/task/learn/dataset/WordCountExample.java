package com.luoj.task.learn.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import org.apache.flink.table.api.*;
import static org.apache.flink.table.api.Expressions.*;
/**
 * @author lj.michale
 * @description Flink DataSet API计算
 *              http://flink.iteblog.com/dev/batch/index.html
 * @date 2021-07-27
 */
public class WordCountExample {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.fromElements(
                "Who's there?",
                "I think I hear them. Stand, ho! Who's there?");

        DataSet<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);

        wordCounts.print();

        // Source From MySQL
//        DataSet<Tuple2<String, Integer>> dbData = env.createInput(
//                // create and configure input format
//                JDBCInputFormat.buildJDBCInputFormat()
//                        .setDrivername("com.mysql.cj.jdbc.Driver")
//                        .setDBUrl("jdbc:mysql://localhost:3306/analysis?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC")
//                        .setUsername("root")
//                        .setPassword("abc1314520")
//                        .setQuery("select name, age from persons")
//                        .finish(),
//                // specify type information for DataSet
//                new TupleTypeInfo(Tuple2.class, STRING_TYPE_INFO, INT_TYPE_INFO)
//        );

        DataSource<Row> dbData = env.createInput(
                JDBCInputFormat.buildJDBCInputFormat()
                        .setDrivername("com.mysql.cj.jdbc.Driver")
                        .setDBUrl("jdbc:mysql://localhost:3306/analysis?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC")
                        .setUsername("root")
                        .setPassword("abc1314520")
                        .setQuery("select * from tianchi_mobile_recommend_train_user")
                        .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
                        .finish());

        dbData.print();

        // 之前老的api处理方式
//        final BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
//        tableEnv.registerDataSet("t_tianchi_mobile_recommend_train_user", dbData);
//        Table table = tableEnv.sqlQuery("select * from t_tianchi_mobile_recommend_train_user");
//        DataSet<Row> dataSet = tableEnv.toDataSet(table, Row.class);
//        dataSet.print();


        env.execute("WordCountExample");

    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}