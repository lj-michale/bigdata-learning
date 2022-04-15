package com.aurora.window;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.flink.streaming.api.windowing.assigners.DynamicProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 * @author lj.michale
 * @description
 * @date 2022-04-15
 */
@Slf4j
public class FlinkWindowOperatorUnit extends TestCase {


    /**
     * @descri  时间窗口-滚动窗口(Tumbling Windows)
     *
     * 滚动窗口有固定的大小, 窗口与窗口之间不会重叠也没有缝隙.比如,如果指定一个长度为5分钟的滚动窗口, 当前窗口开始计算, 每5分钟启动一个新的窗口。
     * 滚动窗口能将数据流切分成不重叠的窗口，每一个事件只能属于一个窗口。
     *
     * Features:
     * 没有间隙
     * 没有重叠
     * 窗口长度
     *
     */
    @Test
    public void  tumblingWindowsUnit() throws Exception {

        Configuration config = new Configuration();
        config.setInteger("rest.port",8081); // 配置固定端口

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        DataStreamSource<String> source = env.socketTextStream("mydocker", 9999);

        // 扁平化
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = source.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (value, out) -> {
            Arrays.stream(value.split(" ")).forEach(s -> out.collect(Tuple2.of(s, 1L)));
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 聚合
        KeyedStream<Tuple2<String, Long>, String> keyBy = flatMap.keyBy(s -> s.f0);
        // 设置窗口为5秒
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> window = keyBy.window(TumblingProcessingTimeWindows.of(Time.seconds(5l)));
        // 聚合
        window.sum(1).print();

        env.execute();

    }


    /**
     * @descri  时间窗口-滑动窗口(TSliding Windows)
     *
     * 与滚动窗口一样, 滑动窗口也是有固定的长度. 另外一个参数我们叫滑动步长, 用来控制滑动窗口启动的频率.
     * 所以, 如果滑动步长小于窗口长度, 滑动窗口会重叠. 这种情况下, 一个元素可能会被分配到多个窗口中
     * 例如, 滑动窗口长度10分钟, 滑动步长5分钟, 则, 每5分钟会得到一个包含最近10分钟的数据.
     *
     * 固定长度
     * 滑动步长
     * 滑动步长<窗口长度，会造成数据重复
     * 滑动步长>窗口长度，会造成数据丢失
     */
    @Test
    public void slidingWindowsUnit() throws Exception {
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

        //窗口长度为5，没3秒统计一次。
        SlidingProcessingTimeWindows windows = SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(3));
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> window = keyBy.window(windows);

        // 聚合
        window.sum(1).print();

        env.execute();
    }

    /**
     * @descri  时间窗口-会话窗口(Session Windows)
     *
     * 会话窗口分配器会根据活动的元素进行分组. 会话窗口不会有重叠, 与滚动窗口和滑动窗口相比, 会话窗口也没有固定的开启和关闭时间.
     * 如果会话窗口有一段时间没有收到数据, 会话窗口会自动关闭, 这段没有收到数据的时间就是会话窗口的gap(间隔)
     * 我们可以配置静态的gap, 也可以通过一个gap extractor 函数来定义gap的长度. 当时间超过了这个gap, 当前的会话窗口就会关闭, 后序的元素会被分配到一个新的会话窗口
     *
     * 按照key进行分组(划分一个新的窗口)
     * 不会造成数据重叠
     * 没有固定开启和关闭时间
     * 若一段时间内没有数据，窗口自动关闭
     * 可以设置静态gap和动态gap
     *
     */
    @Test
    public void sessionWindowsUnit() throws Exception {

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
        // 设置session时间为3秒
        /**
         * session会将gap时间范围内的窗口合并成一个窗口。
         * 例如：a1,a2,a3,a4,b1,b2,b3,b4
         */
        // 静态 gap
//        ProcessingTimeSessionWindows windows = ProcessingTimeSessionWindows.withGap(Time.seconds(3));
        // 动态 gap
        DynamicProcessingTimeSessionWindows<Object> windows =
                ProcessingTimeSessionWindows.withDynamicGap(e-> new Random().nextInt(3000));

        WindowedStream<Tuple2<String, Long>, String, TimeWindow> window = keyBy.window(windows);

        // 聚合
        window.sum(1).print();

        env.execute();
    }

    /**
     * @descri  时间窗口-全局窗口(Global Windows)
     *
     * 全局窗口分配器会分配相同key的所有元素进入同一个 Global window. 这种窗口机制只有指定自定义的触发器时才有用. 否则, 不会做任何计算, 因为这种窗口没有能够处理聚集在一起元素的结束点.
     *
     * 需要指定触发器
     *
     */
    @Test
    public void globalWindowsUnit() throws Exception {

        Configuration config = new Configuration();
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
        window.process(new ProcessWindowFunction<Tuple2<String, Long>, Object, String, GlobalWindow>() {
            /**
             * @param key 聚合元素
             * @param context 上下文
             * @param elements 窗口所有的元素
             * @param out 收集器
             * @throws Exception
             */
            @Override
            public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Object> out) throws Exception {
                ArrayList<Tuple2<String, Long>> list =new ArrayList<>();
                elements.forEach(list::add);
                String msg=String.format("key=%s,window=%s ,data=%s",key, context.window(),list);
                out.collect(msg);
            }
        }).print();

        env.execute();

    }


    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 基于元素个数的窗口

    /**
     * @descr
     * 基于元素个数的窗口
     * 按照指定的数据条数生成一个Window，与时间无关
     *
     * 若有N个元素
     * a、b、c、b、a、c、a、c、b、a、a、b、c
     * 那么会划为三个窗口，不同元素划分为一个窗口(a窗口，b窗口，c窗口)。
     *
     *
     *
     */

    /**
     * @descri  时间窗口-滚动窗口
     *
     * 默认的CountWindow是一个滚动窗口，只需要指定窗口大小即可，当元素数量达到窗口大小时，就会触发窗口的执行。
     */
    @Test
    public void countTumblingWindowsUnit() throws Exception {
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
        WindowedStream<Tuple2<String, Long>, String, GlobalWindow> window = keyBy.countWindow(3);
        // 聚合
        window.sum(1).print();
        env.execute();
    }

    /**
     * @descri  时间窗口-滑动窗口
     *
     * 滑动窗口和滚动窗口的函数名是完全一致的，只是在传参数时需要传入两个参数，一个是window_size，一个是sliding_size。下面代码中的sliding_size设置为了2，也就是说，每收到两个相同key的数据就计算一次，每一次计算的window范围最多是3个元素。
     */
    @Test
    public void countSlidingWindowsUnit() throws Exception {
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
        WindowedStream<Tuple2<String, Long>, String, GlobalWindow> window = keyBy.countWindow(5, 3);
        // 聚合
        window.sum(1).print();
        env.execute();
    }



}
