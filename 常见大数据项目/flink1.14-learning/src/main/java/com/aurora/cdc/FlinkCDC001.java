package com.aurora.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * @author lj.michale
 * @date 2022-01-19
 */
public class FlinkCDC001 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


    }

}
