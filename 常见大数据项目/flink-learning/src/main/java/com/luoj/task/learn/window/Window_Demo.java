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
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @program flink-demo
 * @description: 演示基于时间滚动和滑动的时间窗口
 * @author: eralj.michaleinm
 * @create: 2021/03/04 15:16
 */
public class Window_Demo {

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
         * 需求1:每5秒钟统计一次，最近5秒钟内，各个路口/信号灯通过红绿灯汽车的数量--基于时间的滚动窗口
         */
        // carDS.keyBy(cartInfo -> cartInfo.getSensorId());
        KeyedStream<CartInfo, String> keyedStream = carDS.keyBy(CartInfo::getSensorId);
        //keyedStream.timeWindow(Time.seconds(5)); // 过时
        SingleOutputStreamOperator<CartInfo> result1 = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum("count");

        /**
         * 需求2:每5秒钟统计一次，最近10秒钟内，各个路口/信号灯通过红绿灯汽车的数量--基于时间的滑动窗口
         */
        SingleOutputStreamOperator<CartInfo> result2 = keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).sum("count");

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
        private String sensorId;          //信号灯id
        private Integer count;            //通过该信号灯的车的数量
    }
}

