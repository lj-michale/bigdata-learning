package com.luoj.task.learn.tableapi;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author lj.michale
 * @description https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/common.html
 *
 * @date 2021-04-12
 */
public class TableApi {


    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);

        EnvironmentSettings mySetting = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(mySetting);

        // create a TableEnvironment for specific planner batch or streaming
        //TableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section
        //
        //// create an input Table
        //tableEnv.executeSql("CREATE TEMPORARY TABLE table1 ... WITH ( 'connector' = ... )");
        //// register an output Table
        //tableEnv.executeSql("CREATE TEMPORARY TABLE outputTable ... WITH ( 'connector' = ... )");
        //
        //// create a Table object from a Table API query
        //Table table2 = tableEnv.from("table1").select(...);
        //// create a Table object from a SQL query
        //Table table3 = tableEnv.sqlQuery("SELECT ... FROM table1 ... ");
        //
        //// emit a Table API result Table to a TableSink, same for SQL result
        //TableResult tableResult = table2.executeInsert("outputTable");
        //tableResult...

        // create an input Table


    }
}
