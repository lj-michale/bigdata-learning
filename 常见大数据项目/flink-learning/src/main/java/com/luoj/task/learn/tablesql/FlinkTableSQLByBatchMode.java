package com.luoj.task.learn.tablesql;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author lj.michale
 * @description
 * @date 2021-04-12
 */
@Slf4j
public class FlinkTableSQLByBatchMode {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings mySetting = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);

        TableEnvironment tEnv = TableEnvironment.create(mySetting);

        /**
         * Hive Catalog
         * */
        String name = "hive";
        String defaultDatabase = "stream_tmp";
        String hiveConfDir = "E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\src\\main\\resources";
        String version = "1.1.0";

        log.info("name:{}, defaultDatabase:{}, hiveConfDir:{}, version:{}", name, defaultDatabase, hiveConfDir, version);

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tEnv.registerCatalog("hive", hive);
        tEnv.useCatalog("hive");

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        String dataAnalysisSQL = "SELECT t2.manufacturer, t1.app_key, t1.platform, t1.channel, t1.app_alive, t1.awake_type, t1.uid, t1.from_package\n" +
                "FROM edw.android_awake_target_log t1\n" +
                "LEFT JOIN edw.device_info_fact t2 ON t1.uid = t2.uid\n" +
                "WHERE t2.data_date =20201218";

        TableResult tableResult = tEnv.executeSql(dataAnalysisSQL);
        tableResult.print();

        // TableResult -> DataSet

    }
}
