package com.luoj.task.example.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @program flink-demo
 * @description: 演示DataStream-Source-基于集合
 * 一般用于学习测试时编造数据时使用
 * env.fromElements(可变参数);
 * env.fromColletion(各种集合);
 * env.generateSequence(开始,结束);
 * env.fromSequence(开始,结束);
 * @author: lj
 * @create: 2021/02/20 17:19
 */
public class Source_Demo01_Collection {

    public static void main(String[] args) throws Exception {

        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 2.source
        DataStreamSource<String> source1 = env.fromElements("spark flink hello", "flink java kafka", "HBase scala flink");
        DataStreamSource<String> source2 = env.fromCollection(Arrays.asList("spark flink hello", "flink java kafka", "HBase scala flink"));
        DataStreamSource<Long> source3 = env.generateSequence(1, 100);
        DataStreamSource<Long> source4 = env.fromSequence(1, 100);
        // 3.transformation

        // 4.sink
        source1.print();
        source2.print();
        source3.print();
        source4.print();

        // 5.execute
        env.execute();
    }
}