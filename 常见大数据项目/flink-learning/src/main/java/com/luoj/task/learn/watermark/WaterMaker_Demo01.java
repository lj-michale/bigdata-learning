package com.luoj.task.learn.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

/**
 * @program flink-demo
 * @description: 演示基于事件时间的窗口计算+waterMaker解决一定程度上的数据乱序问题或数据延迟问题
 * @author: lj.michale
 * @create: 2021/03/04 20:08
 */
public class WaterMaker_Demo01 {

    public static void main(String[] args) throws Exception {
        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 2.source
        DataStreamSource<Order> orderDS = env.addSource(new SourceFunction<Order>() {
            private Boolean flag = true;
            Random random = new Random();

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                while (flag) {
                    String orderId = UUID.randomUUID().toString();
                    int userId = random.nextInt(2);
                    int money = random.nextInt(101);
                    // 随机模拟延迟
                    long eventtime = System.currentTimeMillis() - random.nextInt(5) * 1000;
                    ctx.collect(new Order(orderId, userId, money, eventtime));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });
        //3.Transformation
        // 基于事件事件进行窗口计算+watermaker
        // 求每个用户的订单总金额，每隔5s计算最近5s的数据
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //在新版本中默认就是EventTime
        SingleOutputStreamOperator<Order> orderDSWithWaterMaker = orderDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((order, timestamp) -> order.eventTime));
        // 4.sink
        SingleOutputStreamOperator<Order> result = orderDSWithWaterMaker.keyBy(Order::getUserId).window(TumblingEventTimeWindows.of(Time.seconds(5))).sum("money");
        result.print();
        // 5.execute
        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long eventTime;
    }
}
