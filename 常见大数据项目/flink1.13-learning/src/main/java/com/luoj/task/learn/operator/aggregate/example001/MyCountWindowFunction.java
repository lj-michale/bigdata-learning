package com.luoj.task.learn.operator.aggregate.example001;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author lj.michale
 * @description  自定义窗口函数
 *   自定义窗口函数，封装成字符串
 *   第一个参数是上面MyCountAggregate的输出，就是商品的访问量统计
 *   第二个参数 输出 这里为了演示 简单输出字符串
 *   第三个就是 窗口类 能获取窗口结束时间
 * @date 2021-05-27
 */
public class MyCountWindowFunction implements WindowFunction<Long,String,String, TimeWindow> {

    @Override
    public void apply(String productId, TimeWindow window, Iterable<Long> input, Collector<String> out) throws Exception {
        /*商品访问统计输出*/
        /*out.collect("productId"productId,window.getEnd(),input.iterator().next()));*/
        out.collect("----------------窗口时间：" + window.getEnd());
        out.collect("商品ID: " + productId + "  浏览量: " + input.iterator().next());
    }
}
