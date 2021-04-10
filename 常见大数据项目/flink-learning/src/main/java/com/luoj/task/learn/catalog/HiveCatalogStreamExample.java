//package com.luoj.task.learn.catalog;
//
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.SqlDialect;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.catalog.hive.HiveCatalog;
//
///**
// * @description 基于FlinkStreaming-SQL流批一体化的实时数仓
// * @author lj.michale
// * @date 2021-01-30
// * */
//@Slf4j
//public class HiveCatalogStreamExample {
//
//    public static void main(String[] args) throws Exception {
//
//        log.info(" ################ 开始数据ETL ################ ");
//
//        /**
//         * Init Flink Streaming Env
//         * */
//        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
//
//        bsEnv.enableCheckpointing(5000);
//        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        /**
//         * Hive Catalog
//         * */
//        String name = "hive";
//        String defaultDatabase = "stream_tmp";
//        String hiveConfDir = "E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\src\\main\\resources";
//        String version = "1.1.0";
//
//        log.info("name:{}, defaultDatabase:{}, hiveConfDir:{}, version:{}", name, defaultDatabase, hiveConfDir, version);
//
//        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
//        tEnv.registerCatalog("hive", hive);
//        tEnv.useCatalog("hive");
//
//        ///////////////////////////// DDL /////////////////////////////////////
//        /**
//         * 创建Kafka视图
//         * */
//        log.info(" ############# 开始处理Kafka数据流 ############# ");
//        // 使用 default dialect
//        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
//        String CREAT_KAFKA_TABLE_DDL = "CREATE TABLE kafkaTable55 (\n" +
//                " code STRING," +
//                " total_emp INT ," +
//                " ts bigint ," +
//                " r_t AS TO_TIMESTAMP(FROM_UNIXTIME(ts,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss'),\n" +
//                " WATERMARK FOR r_t AS r_t - INTERVAL '5' SECOND " +
//                " ) WITH (" +
//                " 'connector' = 'kafka'," +
//                " 'topic' = 'jiguang_001'," +
//                " 'properties.bootstrap.servers' = '172.17.11.31:9092,172.17.11.30:9092'," +
//                " 'properties.group.id' = 'test1'," +
//                " 'format' = 'json'," +
//                " 'scan.startup.mode' = 'latest-offset'" +
//                ")";
//        log.info("创建KAFKA视图语句: {}", CREAT_KAFKA_TABLE_DDL);
//        tEnv.executeSql(CREAT_KAFKA_TABLE_DDL);
//
//        /**
//         * 创建Hive表
//         * */
//        log.info(" ############# 将Kafka数据流落盘到HIVE ############# ");
//        // 使用hive dialect
//        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//        // 如果hive中已经存在了相应的表，则这段代码省略
//        String CREAT_HIVE_TABLE_DDL = "CREATE TABLE fs_table33 (\n" +
//                "  f_random_str STRING,\n" +
//                "  f_sequence INT" +
//                "  ) partitioned by (dt string, hr string) " +
//                "  stored as PARQUET " +
//                "  TBLPROPERTIES (\n" +
//                "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n" +
//                "  'sink.partition-commit.delay'='5 s',\n" +
//                "  'sink.partition-commit.trigger'='partition-time',\n" +
////                "  'sink.partition-commit.delay'='1 m',\n" +
//                "  'sink.partition-commit.policy.kind'='metastore,success-file'" +
//                ")";
//        log.info("创建HIVE-DDL语句: {}", CREAT_HIVE_TABLE_DDL);
//        tEnv.executeSql(CREAT_HIVE_TABLE_DDL);
//
//        ///////////////////////////// DQL-进行数据ETL处理 /////////////////////////////////////
//        /**
//         * 将处理之后的数据流式插入Hive
//         * */
//        log.info("开始往Hive表中插入数据");
//        String INSERT_DDL = "INSERT INTO fs_table33 " +
//                "SELECT code, total_emp, DATE_FORMAT(r_t, 'yyyy-MM-dd'), DATE_FORMAT(r_t, 'HH') FROM kafkaTable55";
//        tEnv.executeSql(INSERT_DDL);
//
//
//    }
//
//}
