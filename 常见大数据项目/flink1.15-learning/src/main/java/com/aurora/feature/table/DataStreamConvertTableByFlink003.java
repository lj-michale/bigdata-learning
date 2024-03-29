package com.aurora.feature.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @descri DS流与表转换
 *
 * @author lj.michale
 * @date 2022-04-14
 */
public class DataStreamConvertTableByFlink003 {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Flink1.14不需要设置Planner
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<Order> dataStreamSource = env.addSource(new RichSourceFunction<Order>() {
            private Boolean isRunning = true;
            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random=new Random();
                while(isRunning) {
                    Order order = new Order(UUID.randomUUID().toString(), UUID.randomUUID().toString(),random.nextInt(101),System.currentTimeMillis());
                    TimeUnit.SECONDS.sleep(1);
                    ctx.collect(order);
                }
            }

            @Override
            public void cancel() {
                isRunning=false;
            }

        });

        tableEnv.createTemporaryView("Orders", dataStreamSource);
        // schema (a, b, c, rowtime)
        Table orders = tableEnv.from("Orders");
        orders.printSchema();
        Table result = orders.filter(and(
                            $("a").isNotNull(),
                            $("b").isNotNull(),
                            $("c").isNotNull()
                        ))
                .select($("a").lowerCase().as("a"), $("b"), $("rowtime"))
                .window(Tumble.over(lit(1).hours()).on($("rowtime")).as("hourlyWindow"))
                .groupBy($("hourlyWindow"), $("a"))
                .select($("a"), $("hourlyWindow").end().as("hour"), $("b").avg().as("avgBillingAmount"));

//        DataStream<Tuple2<Boolean, Row>> resultDS = tableEnv.toRetractStream(table, Row.class);
        DataStream<Tuple2<Boolean, Row>> resultDS2 = tableEnv.toRetractStream(result, Row.class);
//        resultDS.print();
        resultDS2.print();

        env.execute("DataStreamConvertTableByFlink003");

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order{
        public String a;
        public String b;
        public Integer c;
        public Long rowtime;
    }
}
