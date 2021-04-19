package com.luoj.task.learn.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author lj.michale
 * @description DataSet Api实现WordCount
 * @date 2021-04-19
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> lines = env.fromElements("itcast hadoop spark","itcast hadoop spark","itcast hadoop","itcast");

        ////////////////////////// Transformation Start ///////////////////////////////////////
        FlatMapOperator<String, String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            /**
             * @descr 进行String切割
             * @param s 输入值
             * @param collector
             */
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] arr = s.split(" ");
                for(String word : arr) {
                    collector.collect(word);
                }
            }
        });

        DataSet< Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                // s 就是每一个单词
                return Tuple2.of(s, 1);
            }
        });

        UnsortedGrouping<Tuple2<String, Integer>> grouped = wordAndOne.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> result = grouped.sum(1);

        ////////////////////////// Transformation End ///////////////////////////////////////

        result.print();

    }

}
