package com.luoj.task.learn.hive;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @author lj.michale
 * @description Hive批处理数据分析
 * @date 2021-07-27
 */
@Slf4j
public class DataSetDataAnalysisExample001 {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));



        env.execute("DataSetDataAnalysisExample001");

    }

}
