package com.aurora.stream;

import com.aurora.source.Order;
import com.aurora.source.OrderSource;
import lombok.extern.slf4j.Slf4j;
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
 * @descr  Flink1.14 流作业
 *
 * @author lj.michale
 * @date 2021-12-07
 */
@Slf4j
public class FlinkStreamExample001 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // 获取订单实时数据
        DataStreamSource<Order> orderSource =
                new DataStreamSource<>(
                        env,
                        TypeInformation.of(Order.class),
                        new StreamSource<>(new OrderSource(500000000)),
                        true,
                        "Order-Source",
                        Boundedness.BOUNDED);

        ////////////////////////////////  数据处理转换 ////////////////////////////////////
        orderSource.print();
        DataStream<Order> orderDataStream = orderSource.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Order>noWatermarks()
                                .withTimestampAssigner((order, ts) -> order.getOrderTime().toEpochSecond(ZoneOffset.of("+8"))));

        DataStream<Tuple3<Integer, Long, Instant>> orderCountStream =
                orderDataStream
                        .keyBy(Order::getType)
                        .window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .aggregate(new FlinkStreamExample001.CountAggregatorFunction<>(), new FlinkStreamExample001.OrderCountWindowFunction());

        tableEnv.createTemporaryView("t_order_count", orderCountStream);
        tableEnv.sqlQuery("select * from t_order_count").execute().print();

        ////////////////////////////////  数据Sink ////////////////////////////////////
//        tableEnv.toDataStream(orderStatJoinTable).addSink(
//                new SinkFunction<Row>() {
//                    @Override
//                    public void invoke(Row value, Context context) throws Exception {
//                        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>Sink-1: " + value);
//                    }
//                });

        env.execute("FlinkStreamExample001");

    }

    /**
     * @descr 订单聚合函数
     * @param
     */
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

    /**
     * @descr 订单统计窗口函数
     * @param
     */
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
                    l -> collector.collect(
                                    new Tuple3<>(
                                            integer,
                                            l,
                                            Instant.ofEpochSecond(timeWindow.getEnd()))));
        }
    }




}
