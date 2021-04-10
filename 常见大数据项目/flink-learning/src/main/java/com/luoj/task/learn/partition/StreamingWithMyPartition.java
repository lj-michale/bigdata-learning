package com.luoj.task.learn.partition;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;

/**
 * @author lj.michale
 * @description
 * @date 2021-04-10
 */
@Slf4j
public class StreamingWithMyPartition {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // DataStreamSource<Long> textDataStream = env.addSource(new MyParalleSource())
        // DataStreamSource<Long> streamSource = env.addSource(new MyParalleSource());

    }

}
