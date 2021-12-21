package com.aurora.stream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @descr
 *
 * @author lj.michale
 * @date 2021-12-06
 */
public class BlockedSQLSourcesJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());

        // Add a sample order table
        tableEnv.executeSql(
                "CREATE TEMPORARY TABLE orders (\n"
                        + "    order_number BIGINT,\n"
                        + "    price        DECIMAL(32,2),\n"
                        + "    name   STRING,\n"
                        + "    type   INT,\n"
                        + "    order_time   TIMESTAMP(3)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'number-of-rows' = '200',\n"
                        + " 'fields.name.length' = '2',\n"
                        + " 'fields.type.min'='1',\n"
                        + " 'fields.type.max'='5',\n"
                        + " 'rows-per-second' = '50'"
                        + ")");

        Table orderTypeCount =
                tableEnv.sqlQuery(
                        "select type, TUMBLE_END(order_time, INTERVAL '1' second) as ts, count(*) "
                                + "as the_count from orders group by type, TUMBLE(order_time, INTERVAL '1' second)");
        DataStream<Row> orderStream =
                tableEnv.toDataStream(orderTypeCount)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Row>noWatermarks()
                                        .withTimestampAssigner(
                                                (row, ts) ->
                                                        ((LocalDateTime) row.getField("ts"))
                                                                .toEpochSecond(
                                                                        ZoneOffset.of("+8"))));

        // Let's sample another type attribute table
        tableEnv.executeSql(
                "CREATE TEMPORARY TABLE type_stat (\n"
                        + "    avg_price        DECIMAL(32,2),\n"
                        + "    type   INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'number-of-rows' = '5',\n"
                        + " 'fields.type.kind' = 'sequence',\n"
                        + " 'fields.type.start'= '1',\n"
                        + " 'fields.type.end'= '5'\n"
                        + ")");
        Table typeStatTable = tableEnv.sqlQuery("select * from type_stat");
        DataStream<Row> typeStatStream =
                tableEnv.toDataStream(typeStatTable)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Row>noWatermarks()
                                        .withTimestampAssigner((event, ts) -> 0));

        // 1. The first result
        orderStream
                .join(typeStatStream)
                .where(row -> row.getField("type"))
                .equalTo(row -> row.getField("type"))
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(
                        (JoinFunction<Row, Row, Row>)
                                (row, row2) ->
                                        Row.of(
                                                row.getField("type"),
                                                row.getField("ts"),
                                                row.getField("the_count"),
                                                row2.getField("avg_price")))
                .addSink(
                        new SinkFunction<Row>() {
                            @Override
                            public void invoke(Row value, Context context) throws Exception {
                                System.out.println("Sink-1: " + value);
                            }
                        });

        // 2. The second result, lets transform the order stream back to sql
        Table newOrderTable =
                tableEnv.fromDataStream(
                        orderStream.map(
                                new MapFunction<Row, Tuple2<Integer, Long>>() {
                                    @Override
                                    public Tuple2<Integer, Long> map(Row row) throws Exception {
                                        return new Tuple2<>(
                                                (int) row.getField("type"),
                                                (long) row.getField("the_count"));
                                    }
                                }),
                        $("type"),
                        $("the_count"));
        tableEnv.createTemporaryView("new_order", newOrderTable);
        Table sum2Table =
                tableEnv.sqlQuery(
                        "select type, sum(the_count) as the_count from new_order group by type");
        tableEnv.toDataStream(sum2Table)
                .addSink(
                        new SinkFunction<Row>() {
                            @Override
                            public void invoke(Row value, Context context) throws Exception {
                                System.out.println("Sink-2: " + value);
                            }
                        });

        env.execute();
    }
}
