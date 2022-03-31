package com.aurora.feature.datastream;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @descri  Flink1.14.4流批一体体验, 流批一体化下的批处理程序
 *
 * @author lj.michale
 * @date 2022-03-31
 */
@Slf4j
public class FlinkDatasetDemo1 {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 老版本是返回DataSet
        DataSource<String> data = env.fromElements("hehe", "haha", "哈哈", "哈哈");
        String[] str1 = {"hehe1", "haha1", "哈哈1", "哈哈1"};
        // 老版本是返回DataSet
        DataSource<String> data1 = env.fromElements(str1);

        AggregateOperator<Tuple2<String, Integer>> result = data.flatMap(new FlatMapFunction1())
                .groupBy(0).sum(1);
        result.print();

        System.out.println("**************************");

        SortPartitionOperator<Tuple2<String, Integer>> result1 = data1.flatMap(new FlatMapFunction2())
                .groupBy(0).sum(1).sortPartition(1, Order.DESCENDING);
        result1.print();

    }

    private static class FlatMapFunction1 implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String cell : value.split("\\s+")) {
                out.collect(Tuple2.of(cell, 1));
            }
        }
    }

    private static class FlatMapFunction2 implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] split = value.split("\\s+");
            for (int i = 0; i < split.length; i++) {
                out.collect(new Tuple2<>(split[i], 1));
            }
        }
    }
}
