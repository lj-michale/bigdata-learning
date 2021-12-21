package com.aurora.stream;

import com.aurora.source.TypeStat;
import com.aurora.source.TypeStatSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @descr
 *
 * @author lj.michale
 * @date 2021-12-06
 */
public class MixBlockedSourcesJob {

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

        DataStreamSource<TypeStat> typeStateSource =
                new DataStreamSource<>(
                        env,
                        TypeInformation.of(TypeStat.class),
                        new StreamSource<>(new TypeStatSource()),
                        false,
                        "Type State Source",
                        Boundedness.BOUNDED);
        DataStream<TypeStat> typeStatDataStream =
                typeStateSource.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TypeStat>noWatermarks()
                                .withTimestampAssigner((order, ts) -> 0L));

        // 1. First try sql with this case
        Table orderTypeCount =
                tableEnv.sqlQuery(
                        "select type, TUMBLE_END(order_time, INTERVAL '1' second) as ts, count(*) "
                                + "as the_count from orders group by type, TUMBLE(order_time, INTERVAL '1' second)");
        tableEnv.createTemporaryView("order_count", orderTypeCount);
        tableEnv.createTemporaryView("type_stat", typeStatDataStream);
        Table orderStatJoinTable =
                tableEnv.sqlQuery(
                        "select oc.type as type, oc.the_count as the_count, stat.avgPrice as avg_price from order_count oc join type_stat stat on oc.type = stat.type");
        tableEnv.toDataStream(orderStatJoinTable)
                .addSink(
                        new SinkFunction<Row>() {
                            @Override
                            public void invoke(Row value, Context context) throws Exception {
                                System.out.println("Sink-1: " + value);
                            }
                        });

        // 2. And also try datastream in this case
        DataStream<Row> orderCountStream =
                tableEnv.toDataStream(orderTypeCount)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Row>noWatermarks()
                                        .withTimestampAssigner((order, ts) -> 0L));
        orderCountStream
                .join(typeStatDataStream)
                .where(row -> (int) row.getField("type"))
                .equalTo(TypeStat::getType)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(
                        (JoinFunction<Row, TypeStat, Row>)
                                (row, row2) ->
                                        Row.of(
                                                row.getField("type"),
                                                row.getField("ts"),
                                                row.getField("the_count"),
                                                row2.getAvgPrice()))
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
