package com.aurora.feature.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Instant;

/**
 * @descri DS流与表转换
 *
 * @author lj.michale
 * @date 2022-04-14
 */
public class DataStreamConvertTableByFlink002 {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Flink1.14不需要设置Planner
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // create a DataStream
        DataStream<User> dataStream = env.fromElements(
                new User("Alice", 4),
                new User("Bob", 6),
                new User("Alice", 10));

        // since fields of a RAW type cannot be accessed, every stream record is treated as an atomic type
        // leading to a table with a single column `f0`

        Table table = tableEnv.fromDataStream(dataStream);
        table.printSchema();
        // prints:
        // (
        //  `f0` RAW('User', '...')
        // )

        // instead, declare a more useful data type for columns using the Table API's type system
        // in a custom schema and rename the columns in a following `as` projection

        Table table2 = tableEnv
                .fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .column("f0", DataTypes.of(User.class))
                                .build())
                .as("user");
        table2.printSchema();
        // prints:
        // (
        //  `user` *User<`name` STRING,`score` INT>*
        // )

        // data types can be extracted reflectively as above or explicitly defined

        Table table3 = tableEnv
                .fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .column(
                                        "f0",
                                        DataTypes.STRUCTURED(
                                                User.class,
                                                DataTypes.FIELD("name", DataTypes.STRING()),
                                                DataTypes.FIELD("score", DataTypes.INT())))
                                .build())
                .as("user");
        table3.printSchema();
        // prints:
        // (
        //  `user` *User<`name` STRING,`score` INT>*
        // )

        env.execute("DataStreamConvertTableByFlink001");

    }

    // the DataStream API does not support immutable POJOs yet,
    // the class will result in a generic type that is a RAW type in Table API by default
    public static class User {

        public final String name;

        public final Integer score;

        public User(String name, Integer score) {
            this.name = name;
            this.score = score;
        }
    }


}
