//package com.luoj.task.learn.tablesql;
//
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.table.api.*;
//import org.apache.flink.table.catalog.hive.HiveCatalog;
//import org.apache.flink.table.types.DataType;
//
//import java.io.IOException;
//
///**
// * @author lj.michale
// * @description
// * @date 2021-04-10
// */
//public class TmpTableExample {
//    public static void main(String[] args) throws IOException {
//
//
//        ParameterToolFactory parameterToolFactory = new ParameterToolFactory();
//        ParameterTool tool = parameterToolFactory.createParameterTool();
//
//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .useBlinkPlanner()
//                .inBatchMode()
//                .build();
//
//        TableEnvironment tableEnv = TableEnvironment.create(settings);
//        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
//
//        HiveCatalog testCataLog = new HiveCatalog(CataLogEnum.TEST.getCataLogName(), CataLogEnum.TEST.getDbName(),
//                tool.get(FlinkProperEnum.FLINK_HIVE_CONF_DIR.key));
//
//
//        tableEnv.registerCatalog(CataLogEnum.TEST.getCataLogName(), testCataLog);
//
//        tableEnv.useCatalog(CataLogEnum.TEST.getCataLogName());
//        String sql = "SELECT * FROM test.student as t1 JOIN test2.class t2 ON t1.id = t2.student_id";
//
//
//        JdbcOptions options = JdbcOptions.builder()
//                .setDBUrl(tool.get(FlinkProperEnum.FLINK_MYSQL_CUSTOM_DATASOURCE_NEWBI_URL.key))
//                .setDriverName(tool.get(FlinkProperEnum.FLINK_MYSQL_CUSTOM_DATASOURCE_NEWBI_DRIVER_CLASS_NAME.key))
//                .setUsername(tool.get(FlinkProperEnum.FLINK_MYSQL_CUSTOM_DATASOURCE_NEWBI_USERNAME.key))
//                .setPassword(tool.get(FlinkProperEnum.FLINK_MYSQL_CUSTOM_DATASOURCE_NEWBI_PASSWORD.key))
//                .setTableName("mysql_project_test")
//                .setDialect(new MySQLDialect())
//                .build();
//
//        String[] fieldNames = {"student_id", "student_name", "student_curriculum", "student_score",
//                "student_dt", "class_id", "class_student_id", "class_name", "class_size", "class_dt"};
//
//        DataType[] fieldTypes = {DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.DOUBLE(),
//                DataTypes.STRING(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.INT(),
//                DataTypes.STRING()};
//
//        String[] keys = {"student_id", "class_id"};
//
//        TableSchema schema = TableSchema.builder()
//                .fields(fieldNames, fieldTypes)
//                .build();
//
//
//        JdbcUpsertTableSink tableSink = JdbcUpsertTableSink.builder()
//                .setOptions(options)
//                .setTableSchema(schema)
//                .setFlushIntervalMills(1000)
////                .setFlushMaxSize(10)
//                .build();
//
//        tableEnv.registerTableSink("mysql_project_test", tableSink);
//
//        Table result = tableEnv.sqlQuery(sql);
//        StatementSet statementSet = tableEnv.createStatementSet();
//        statementSet.addInsert("mysql_project_test", result);
//        statementSet.execute().print();
//    }
//}
