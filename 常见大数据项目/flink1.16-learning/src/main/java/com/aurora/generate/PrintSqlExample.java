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
public class PrintSqlExample {

    public static void main(String[] args) throws Exception {

        String sql="CREATE TABLE source_table (\n" +
                "    user_id INT,\n" +
                "    cost DOUBLE,\n" +
                "    ts AS localtimestamp,\n" +
                "    WATERMARK FOR ts AS ts\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second'='5',\n" +
                "\n" +
                "    'fields.user_id.kind'='random',\n" +
                "    'fields.user_id.min'='1',\n" +
                "    'fields.user_id.max'='10',\n" +
                "\n" +
                "    'fields.cost.kind'='random',\n" +
                "    'fields.cost.min'='1',\n" +
                "    'fields.cost.max'='100'\n" +
                ")\n";

        String sinkSql="CREATE TABLE print_table (\n" +
                " user_id INT,\n" +
                " cost DOUBLE\n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")";

        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql(sql);

        tEnv.executeSql(sinkSql);
        // 执行查询
        Table table = tEnv.sqlQuery("select user_id,cost from source_table");
        table.executeInsert("print_table");
    }
}