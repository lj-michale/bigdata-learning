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

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

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
                    Order order = new Order(UUID.randomUUID().toString(),random.nextInt(3),random.nextInt(101),System.currentTimeMillis());
                    TimeUnit.SECONDS.sleep(1);
                    ctx.collect(order);
                }
            }

            @Override
            public void cancel() {
                isRunning=false;
            }

        });

        // Option 1:
        // extract timestamp and assign watermarks based on knowledge of the stream  在流转table之前  流要已经设置好watermark
        SingleOutputStreamOperator<Order> outputStream = dataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner( (order,recordTimestamp) -> order.getCreateTime())
        );

        // declare an additional logical field as an event time attribute   "user_action_time" 为table中字段名，可以随意起名
        Table table = tableEnv.fromDataStream(outputStream, $("orderId"),$("userId"),$("money"),$("createTime").rowtime());


        DataStream<Tuple2<Boolean, Row>> resultDS = tableEnv.toRetractStream(table, Row.class);
        resultDS.print();

        env.execute("DataStreamConvertTableByFlink001");

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order{
        public String orderId;
        public Integer userId;
        public Integer money;
        public Long createTime;
    }
}
