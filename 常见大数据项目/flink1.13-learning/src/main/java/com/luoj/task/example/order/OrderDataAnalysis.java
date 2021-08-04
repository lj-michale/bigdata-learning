package com.luoj.task.example.order;

import com.luoj.bean.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-05
 */
@Slf4j
public class OrderDataAnalysis {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataGeneratorSource<Order> orderDataDataGeneratorSource = new DataGeneratorSource<>(new Order.OrderGenerator());
        DataStream<Order> orderDS = env.addSource(orderDataDataGeneratorSource).returns(new TypeHint<Order>() {});
        orderDS.map(new MapFunction<Order, String>() {
            @Override
            public String map(Order o) throws Exception {
                return o.getOrderItemList() + o.getOrderNo() + o.getPaySideId() + o.getPayModeId();
            }
        }).print();

        env.execute("OrderDataAnalysis");
    }

}
