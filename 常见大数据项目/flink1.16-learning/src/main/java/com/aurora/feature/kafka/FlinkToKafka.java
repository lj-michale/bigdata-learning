package com.aurora.feature.kafka;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-05-18
 */
public class FlinkToKafka {

    public static void main(String[] args) {

        String sql = "create table if not exists kafka_table(\n" +
                "  cid STRING,\n" +
                "  crawl_time BIGINT,\n" +
                "  toplist ARRAY<ROW<author STRING,comments BIGINT,hot_value STRING,hot_word STRING,search_url STRING,top INT>>\n" +
                ")with(\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'mzpns',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'jason_flink_test',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json',\n" +
                "  'json.fail-on-missing-field' = 'false',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ")";

        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql(sql);

        // 执行查询
        Table table = tEnv.sqlQuery("select * from kafka_table");
        table.execute().print();

    }

}
