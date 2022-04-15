package com.aurora.window;

import junit.framework.TestCase;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author lj.michale
 * @description
 * @date 2022-04-15
 */
public class FlinkTriggerUnit extends TestCase {


    /**
     * @descr 自定义触发器
     * 输入：
     * a
     * a
     * bb
     * bb
     * bb
     * cc
     * cc
     * cc
     * a
     * cc
     * bb
     *
     */
    @Test
    public void  test1() throws Exception {

        Configuration config=new Configuration();
        config.setInteger("rest.port",8081); // 配置固定端口

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        DataStreamSource<String> source = env.socketTextStream("mydocker", 9999);

        // 扁平化
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = source.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (value, out) -> {
            Arrays.stream(value.split(" ")).forEach(s -> out.collect(Tuple2.of(s, 1L)));
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 聚合
        KeyedStream<Tuple2<String, Long>, String> keyBy = flatMap.keyBy(s -> s.f0);

        // 创建 全局窗口
        WindowedStream<Tuple2<String, Long>, String, GlobalWindow> window = keyBy.window(org.apache.flink.streaming.api.windowing.assigners.GlobalWindows.create());

        // 添加 触发器
        window.trigger(new Trigger<Tuple2<String, Long>, GlobalWindow>() {

            int count=0;

            /**
             * 来一个元素触发一次
             * @param element
             * @param timestamp
             * @param window
             * @param ctx
             * @return
             * @throws Exception
             */
            @Override
            public TriggerResult onElement(Tuple2<String, Long> element,
                                           long timestamp,
                                           GlobalWindow window,
                                           TriggerContext ctx) throws Exception {
                // 三个元素发送一次
                if (count >= 3) {
                    count=0;
                    return TriggerResult.FIRE_AND_PURGE;
                }
                count++;
                // 超时不发送
                return TriggerResult.CONTINUE;
            }

            /**
             * 基于处理时间
             * @param time
             * @param window
             * @param ctx
             * @return
             * @throws Exception
             */
            @Override
            public TriggerResult onProcessingTime(long time,
                                                  GlobalWindow window,
                                                  TriggerContext ctx) throws Exception {
                return null;
            }

            /**
             * 基于事件时间
             * @param time
             * @param window
             * @param ctx
             * @return
             * @throws Exception
             */
            @Override
            public TriggerResult onEventTime(long time,
                                             GlobalWindow window,
                                             TriggerContext ctx) throws Exception {
                return null;
            }

            /**
             * 触发器执行后，清空窗口元素
             * @param window
             * @param ctx
             * @throws Exception
             */
            @Override
            public void clear(GlobalWindow window,
                              TriggerContext ctx) throws Exception {
            }
        });

        window.process(new ProcessWindowFunction<Tuple2<String, Long>, Object, String, GlobalWindow>() {
            /**
             *
             * @param key 聚合元素
             * @param context 上下文
             * @param elements 窗口所有的元素
             * @param out 收集器
             * @throws Exception
             */
            @Override
            public void process(String key,
                                Context context,
                                Iterable<Tuple2<String, Long>> elements,
                                Collector<Object> out) throws Exception {
                ArrayList<Tuple2<String, Long>> list =new ArrayList<>();
                elements.forEach(list::add);
                String msg=String.format("key=%s,window=%s ,data=%s",key, context.window(),list);
                out.collect(msg);
            }
        }).print();

        env.execute();

    }
}
