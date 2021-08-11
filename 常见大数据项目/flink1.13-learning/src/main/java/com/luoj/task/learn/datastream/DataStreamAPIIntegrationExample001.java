package com.luoj.task.learn.datastream;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-11
 */
public class DataStreamAPIIntegrationExample001 {

    public static class User {
        public final String name;
        public final Integer score;
        public User(String name, Integer score) {
            this.name = name;
            this.score = score;
        }
    }

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint");
        env.setStateBackend(new HashMapStateBackend());

        // create a DataStream
        DataStream<User> dataStream = env.fromElements(
                new User("Alice", 4),
                new User("Bob", 6),
                new User("Alice", 10));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // instead, declare a more useful data type for columns using the Table API's type system
        // in a custom schema and rename the columns in a following `as` projection
        Table table = tableEnv
                .fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .column("f0", DataTypes.of(User.class))
                                .build())
                .as("user");
        table.printSchema();
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
        table.printSchema();
        // prints:
        // (
        //  `user` *User<`name` STRING,`score` INT>*
        // )

        // create some DataStream
        DataStream<Tuple2<Long, String>> dataStream2 = env.fromElements(
                Tuple2.of(12L, "Alice"),
                Tuple2.of(0L, "Bob"));

        // === EXAMPLE 1 ===
        // register the DataStream as view "MyView" in the current session
        // all columns are derived automatically
        tableEnv.createTemporaryView("MyView", dataStream);
        tableEnv.from("MyView").printSchema();
        // prints:
        // (
        //  `f0` BIGINT NOT NULL,
        //  `f1` STRING
        // )

        // === EXAMPLE 2 ===
        // register the DataStream as view "MyView" in the current session,
        // provide a schema to adjust the columns similar to `fromDataStream`
        // in this example, the derived NOT NULL information has been removed
        tableEnv.createTemporaryView(
                "MyView",
                dataStream,
                Schema.newBuilder()
                        .column("f0", "BIGINT")
                        .column("f1", "STRING")
                        .build());
        tableEnv.from("MyView").printSchema();
        // prints:
        // (
        //  `f0` BIGINT,
        //  `f1` STRING
        // )

        // === EXAMPLE 3 ===
        // use the Table API before creating the view if it is only about renaming columns
        tableEnv.createTemporaryView(
                "MyView",
                tableEnv.fromDataStream(dataStream).as("id", "name"));
        tableEnv.from("MyView").printSchema();
        // prints:
        // (
        //  `id` BIGINT NOT NULL,
        //  `name` STRING
        // )


    }

}
