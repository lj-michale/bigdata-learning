package com.aurora.generate;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-05-17
 */
public class PrintSqlExample2 {

    public static void main(String[] args) throws Exception {

        String sql="CREATE TABLE kafka_table (\n" +
                "name string,\n" +
                "age int,\n" +
                "city string,\n" +
                "ts BIGINT,\n" +
                "proctime as PROCTIME(),\n" +
                "rt as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "WATERMARK FOR rt AS rt - INTERVAL '5' SECOND\n" +
                ")\n" +
                "WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'test',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',  \n" +
                "    'properties.group.id' = 'jason_flink_test', \n" +
                "    'scan.startup.mode' = 'latest-offset', \n" +
                "    'format' = 'json', \n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'false' \n" +
                ")";

        String sinkSql="CREATE TABLE print_table\n" +
                "(\n" +
                "f1 TIMESTAMP(3),\n" +
                "f2 TIMESTAMP(3),\n" +
                "f3 BIGINT,\n" +
                "f4 STRING\n" +
                ")\n" +
                "WITH (\n" +
                "'connector' = 'print-rate',\n" +
                "'standard-error' = 'false',\n" +
                "'print-rate' = '0.01',\n" +
                "'sink.parallelism' = '4'\n" +
                ")";

        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql(sql);

        tEnv.executeSql(sinkSql);

        // 执行查询
//        Table table = tEnv.sqlQuery("select * from kafka_table");
//        table.execute().print();

        Table table = tEnv.sqlQuery("insert into print_table\n" +
                "select\n" +
                "  window_start,\n" +
                "  window_end,\n" +
                "  count(name),\n" +
                "  name\n" +
                "from table(HOP(table kafka_table,descriptor(proctime),interval '30' second, interval '1' HOUR))\n" +
                "group by window_start,\n" +
                "  window_end,\n" +
                "  name");
        table.executeInsert("print_table");

    }
}