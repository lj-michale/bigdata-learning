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
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @program flink-demo
 * @description: 演示会话窗口
 * @author: lj。michale
 * @create: 2021/03/04 16：12
 */
public class Window_Demo03 {
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

        KeyedStream<CartInfo, String> keyedStream = carDS.keyBy(CartInfo::getSensorId);
        /**
         * 需求:设置会话超时时间为10s,10s内没有数据到来,则触发上个窗口的计算(前提是上一个窗口得有数据!)
         */
        SingleOutputStreamOperator<CartInfo> result = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))).sum("count");
        // 4.sink
        result.print();
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
