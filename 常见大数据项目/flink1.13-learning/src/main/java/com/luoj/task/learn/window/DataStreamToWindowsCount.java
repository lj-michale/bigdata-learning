package com.luoj.task.learn.window;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lj.michale
 * @description 具体需求为5秒内用户订单总数、订单最大金额、最小金额
 * @date 2021-07-26
 */

public class DataStreamToWindowsCount {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);

        DataStreamSource<Order> orderDs = env.addSource(new RichSourceFunction<Order>() {
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

        //需求使用事件时间+Watermark
        //5秒水印
        SingleOutputStreamOperator<Order> orderDSWithWatermark = orderDs
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner( (order,recordTimestamp) -> order.getCreateTime())
        );

        //将DataStream数据注册为表或视图，注意：指定列时，通过rowTime()指定每列时间
        //rowtime指定数据时间
        tableEnv.createTemporaryView("t_order", orderDSWithWatermark,$("orderId"),$("userId"),$("money"),$("createTime").rowtime());

        //sql风格
        //tumble指定5秒窗口
        String sql="select userId,count(*) as orderCount,max(money) as maxMoney,min(money) as minMoney from t_order group by userId,tumble(createTime,Interval '5' SECOND)";
        Table resultTable=tableEnv.sqlQuery(sql);

        //table风格
        Table apiTable=tableEnv.from("t_order")
                .window(Tumble.over(lit(5).second()).on($("createTime"))
                        .as("tumbleWindow"))
                .groupBy($("tumbleWindow"),$("userId"))
                .select($("userId"),
                        $("userId").count().as("totalCount"),
                        $("money").max().as("maxMoney"),
                        $("money").min().as("mixMoney"));


        DataStream<Tuple2<Boolean, Row>> resultDS = tableEnv.toRetractStream(resultTable, Row.class);
        DataStream<Tuple2<Boolean, Row>> resultAPI = tableEnv.toRetractStream(apiTable, Row.class);

        //TODO 4. sink
        resultDS.print("sql数据");
        resultAPI.print("API");

        //TODO 5. execute
        env.execute("DataStreamToWindowsCount");
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
