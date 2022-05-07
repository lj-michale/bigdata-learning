package com.aurora.feature.hdfs;


import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @descri
 * {"user":"Mary","url":"./home","cTime":"2022-02-02 12:00:00"}
 * {"user":"Mary","url":"./prod?id=1","cTime":"2022-02-02 12:00:05"}
 * {"user":"Liz","url":"./home","cTime":"2022-02-02 12:01:00"}
 * {"user":"Bob","cTime":"2022-02-02 12:01:30"}
 * {"user":"Mary","url":"./prod?id=7","cTime":"2022-02-02 12:01:45"}
 *
 * @author lj.michale
 * @date 2022-04-30
 */
public class FlinkTableAPIFromHDFS {

    public static void main(String[] args) {

        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2、创建source table
        final Schema schema = Schema.newBuilder()
                .column("user", DataTypes.STRING())
                .column("url", DataTypes.STRING())
                .column("cTime", DataTypes.STRING())
                .build();

        tEnv.createTemporaryTable("sourceTable", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .format("json")
                .option("path","hdfs://mycluster/data/clicklog/input/click.log")
                .build());

        //3、创建sink table
        tEnv.createTemporaryTable("sinkTable", TableDescriptor.forConnector("print")
                .schema(schema)
                .build());

        //5、输出(包括执行,不需要单独在调用tEnv.execute("job"))
        tEnv.from("sourceTable")
                .select($("user"), $("url"),$("cTime"))
                .executeInsert("sinkTable");
    }
}
