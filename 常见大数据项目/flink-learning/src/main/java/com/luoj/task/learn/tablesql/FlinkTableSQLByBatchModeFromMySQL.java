package com.luoj.task.learn.tablesql;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author lj.michale
 * @description Flink1.11 Batch-SQL处理
 * @date 2021-04-12
 */
public class FlinkTableSQLByBatchModeFromMySQL {

    public static void main(String[] args) {

        EnvironmentSettings mySetting = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);
        // 失败重启,固定间隔,每隔3秒重启1次,总尝试重启10次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 3));
        env.setParallelism(1);



        TableEnvironment tableEnv = TableEnvironment.create(mySetting);



    }
}
