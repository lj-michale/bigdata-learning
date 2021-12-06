package com.aurora.datastream;


import com.aurora.source.Order;
import com.aurora.source.OrderSource;
import com.aurora.source.TypeStat;
import com.aurora.source.TypeStatSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

/**
 * @author lj.michale
 * @description
 * @date 2021-12-06
 */
public class BlockedDataStreamSourcesJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());

        DataStreamSource<Order> orderSource =
                new DataStreamSource<>(
                        env,
                        TypeInformation.of(Order.class),
                        new StreamSource<>(new OrderSource(50)),
                        true,
                        "Order Source",
                        Boundedness.BOUNDED);
        DataStream<Order> orderDataStream =
                orderSource.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Order>noWatermarks()
                                .withTimestampAssigner(
                                        (order, ts) ->
                                                order.getOrderTime()
                                                        .toEpochSecond(ZoneOffset.of("+8"))));

        DataStream<Tuple3<Integer, Long, Instant>> orderCountStream =
                orderDataStream
                        .keyBy(Order::getType)
                        .window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .aggregate(new CountAggregatorFunction<>(), new OrderCountWindowFunction());

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

        tableEnv.createTemporaryView("order_count", orderCountStream);
        tableEnv.createTemporaryView("type_stat", typeStatDataStream);

        // 1. The first result
        Table orderStatJoinTable =
                tableEnv.sqlQuery(
                        "select oc.f0 as type, oc.f1 as the_count, stat.avgPrice as avg_price from order_count oc join type_stat stat on oc.f0 = stat.type");
        tableEnv.toDataStream(orderStatJoinTable)
                .addSink(
                        new SinkFunction<Row>() {
                            @Override
                            public void invoke(Row value, Context context) throws Exception {
                                System.out.println("Sink-1: " + value);
                            }
                        });

        // 2. the second result
        tableEnv.toDataStream(orderStatJoinTable)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Row>noWatermarks()
                                .withTimestampAssigner((row, ts) -> 0L))
                .keyBy(row -> (int) row.getField("type"))
                .window(TumblingEventTimeWindows.of(Time.of(10000, TimeUnit.DAYS)))
                .aggregate(new CountAggregatorFunction<>(), new OrderCountWindowFunction())
                .addSink(
                        new SinkFunction<Tuple3<Integer, Long, Instant>>() {
                            @Override
                            public void invoke(
                                    Tuple3<Integer, Long, Instant> value, Context context)
                                    throws Exception {
                                System.out.println("Sink-2: " + value);
                            }
                        });

        env.execute();
    }

    private static class CountAggregatorFunction<T> implements AggregateFunction<T, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(T t, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    private static class OrderCountWindowFunction
            implements WindowFunction<Long, Tuple3<Integer, Long, Instant>, Integer, TimeWindow> {
        @Override
        public void apply(
                Integer integer,
                TimeWindow timeWindow,
                Iterable<Long> iterable,
                Collector<Tuple3<Integer, Long, Instant>> collector)
                throws Exception {
            iterable.forEach(
                    l ->
                            collector.collect(
                                    new Tuple3<>(
                                            integer,
                                            l,
                                            Instant.ofEpochSecond(timeWindow.getEnd()))));
        }
    }
}
