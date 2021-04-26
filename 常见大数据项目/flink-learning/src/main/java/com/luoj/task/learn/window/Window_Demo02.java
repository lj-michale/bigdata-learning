package com.luoj.task.learn.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @program flink-demo
 * @description: 演示基于数量滚动和滑动的时间窗口
 * @author: lj.michale
 * @create: 2021/03/04 15:47
 */

public class Window_Demo02 {
    public static void main(String[] args) throws Exception {
        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 2.source
        DataStreamSource<String> lines = env.socketTextStream("node1", 9999);
        //3.Transformation
        SingleOutputStreamOperator<CartInfo> carDS = lines.map(new MapFunction<String, CartInfo>() {
            @Override
            public CartInfo map(String s) throws Exception {
                String[] arr = s.split(",");
                return new CartInfo(arr[0], Integer.parseInt(arr[1]));
            }
        });

        /**
         * 需求1:统计在最近5条消息中,各自路口通过的汽车数量,相同的key每出现5次进行统计--基于数量的滚动窗口
         */
        // carDS.keyBy(cartInfo -> cartInfo.getSensorId());
        KeyedStream<CartInfo, String> keyedStream = carDS.keyBy(CartInfo::getSensorId);
        SingleOutputStreamOperator<CartInfo> result1 = keyedStream.countWindow(5).sum("count");
        /**
         * 需求2:统计在最近5条消息中,各自路口通过的汽车数量,相同的key每出现3次进行统计--基于数量的滑动窗口
         */
        SingleOutputStreamOperator<CartInfo> result2 = keyedStream.countWindow(5, 3).sum("count");
        // 4.sink
        result1.print();
        result2.print();
        // 5.execute
        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CartInfo {
        private String sensorId;//信号灯id
        private Integer count;//通过该信号灯的车的数量
    }
}