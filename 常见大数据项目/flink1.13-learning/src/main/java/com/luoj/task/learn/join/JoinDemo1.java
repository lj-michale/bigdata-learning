package com.luoj.task.learn.join;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.HashMap;
import java.util.Map;

/**
 *1、 预加载维表
 * 通过定义一个类实现RichMapFunction，在open()中读取维表数据加载到内存中，在probe流map()方法中与维表数据进行关联。
 * RichMapFunction中open方法里加载维表数据到内存的方式特点如下：
 * 优点：实现简单
 * 缺点：因为数据存于内存，所以只适合小数据量并且维表数据更新频率不高的情况下。虽然可以在open中定义一个定时器定时更新维表，但是还是存在维表更新不及时的情况。
 *
 * 这个例子是从socket中读取的流，数据为用户名称和城市id，维表是城市id、城市名称，
 * 主流和维表关联，得到用户名称、城市id、城市名称
 * 这个例子采用在RichMapfunction类的open方法中将维表数据加载到内存
 **/
public class JoinDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> textStream = env.socketTextStream("localhost", 9000, "\n")
                .map(p -> {
                    //输入格式为：user,1000,分别是用户名称和城市编号
                    String[] list = p.split(",");
                    return new Tuple2<String, Integer>(list[0], Integer.valueOf(list[1]));
                })
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                });
        DataStream<Tuple3<String, Integer, String>> result = textStream.map(new MapJoinDemo1());
        result.print();
        env.execute("joinDemo1");
    }

    static class MapJoinDemo1 extends RichMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, String>> {
        //定义一个变量，用于保存维表数据在内存
        Map<Integer, String> dim;

        @Override
        public void open(Configuration parameters) throws Exception {
            //在open方法中读取维表数据，可以从数据中读取、文件中读取、接口中读取等等。
            dim = new HashMap<>();
            dim.put(1001, "beijing");
            dim.put(1002, "shanghai");
            dim.put(1003, "wuhan");
            dim.put(1004, "changsha");
        }

        @Override
        public Tuple3<String, Integer, String> map(Tuple2<String, Integer> value) throws Exception {
            //在map方法中进行主流和维表的关联
            String cityName = "";
            if (dim.containsKey(value.f1)) {
                cityName = dim.get(value.f1);
            }
            return new Tuple3<>(value.f0, value.f1, cityName);
        }
    }
}