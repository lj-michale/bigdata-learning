package com.luoj.task.learn.datastream;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-29
 */
@Slf4j
public class BatchExecutionBehaviorExample {

    public static void main(String[] args) throws Exception {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStreamSource<String> source = env.fromElements(WORDS);
        DataStream<String> wordStream = source.name("source").flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] tokens = value.toLowerCase().split("\\W+");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        out.collect(token);
                    }
                }
            }
        }).name("flatMap");

        wordStream.print();


        env.execute("BatchExecutionBehaviorExample");

    }

    public static final String[] WORDS = new String[] {
            "To be, or not to be,--that is the question:--",
            "Whether 'tis nobler in the mind to suffer",
            "The slings and arrows of outrageous fortune",
            "And by opposing end them?--To die,--to sleep,--",
            "Be all my sins remember'd."
    };

}
