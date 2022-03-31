package com.aurora.feature.datastream;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @descri  基于DataStream的批处理
 *
 * @author lj.michale
 * @date 2022-03-31
 */
@Slf4j
public class BatchFlinkTask {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);


        env.execute("BatchFlinkTask");

    }

}
