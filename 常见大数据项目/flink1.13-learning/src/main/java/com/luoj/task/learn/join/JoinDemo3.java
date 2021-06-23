package com.luoj.task.learn.join;


import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author lj.michale
 * @description
 * (2) 使用异步IO来提高访问吞吐量
 * Flink与外部存储系统进行读写操作的时候可以使用同步方式，也就是发送一个请求后等待外部系统响应，然后再发送第二个读写请求，这样的方式吞吐量比较低，可以用提高并行度的方式来提高吞吐量，但是并行度多了也就导致了进程数量多了，占用了大量的资源。
 * Flink中可以使用异步IO来读写外部系统，这要求外部系统客户端支持异步IO，不过目前很多系统都支持异步IO客户端。但是如果使用异步就要涉及到三个问题：
 * 超时：如果查询超时那么就认为是读写失败，需要按失败处理；
 * 并发数量：如果并发数量太多，就要触发Flink的反压机制来抑制上游的写入。
 * 返回顺序错乱：顺序错乱了要根据实际情况来处理，Flink支持两种方式：允许乱序、保证顺序。
 * @date 2021-06-23
 */

public class JoinDemo3 {
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


        DataStream<Tuple3<String,Integer, String>> orderedResult = AsyncDataStream
                //保证顺序：异步返回的结果保证顺序，超时时间1秒，最大容量2，超出容量触发反压
                .orderedWait(textStream, new JoinDemo3AyncFunction(), 1000L, TimeUnit.MILLISECONDS, 2)
                .setParallelism(1);

        DataStream<Tuple3<String,Integer, String>> unorderedResult = AsyncDataStream
                //允许乱序：异步返回的结果允许乱序，超时时间1秒，最大容量2，超出容量触发反压
                .unorderedWait(textStream, new JoinDemo3AyncFunction(), 1000L, TimeUnit.MILLISECONDS, 2)
                .setParallelism(1);

        orderedResult.print();
        unorderedResult.print();
        env.execute("joinDemo");
    }

    //定义个类，继承RichAsyncFunction，实现异步查询存储在mysql里的维表
    //输入用户名、城市ID，返回 Tuple3<用户名、城市ID，城市名称>
    static class JoinDemo3AyncFunction extends RichAsyncFunction<Tuple2<String, Integer>, Tuple3<String, Integer, String>> {
        // 链接
        private static String jdbcUrl = "jdbc:mysql://192.168.145.1:3306?useSSL=false";
        private static String username = "root";
        private static String password = "123";
        private static String driverName = "com.mysql.jdbc.Driver";
        java.sql.Connection conn;
        PreparedStatement ps;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            Class.forName(driverName);
            conn = DriverManager.getConnection(jdbcUrl, username, password);
            ps = conn.prepareStatement("select city_name from tmp.city_info where id = ?");
        }

        @Override
        public void close() throws Exception {
            super.close();
            conn.close();
        }

        //异步查询方法
        @Override
        public void asyncInvoke(Tuple2<String, Integer> input, ResultFuture<Tuple3<String,Integer, String>> resultFuture) throws Exception {
            // 使用 city id 查询
            ps.setInt(1, input.f1);
            ResultSet rs = ps.executeQuery();
            String cityName = null;
            if (rs.next()) {
                cityName = rs.getString(1);
            }
            List list = new ArrayList<Tuple2<Integer, String>>();
            list.add(new Tuple3<>(input.f0,input.f1, cityName));
            resultFuture.complete(list);
        }

        //超时处理
        @Override
        public void timeout(Tuple2<String, Integer> input, ResultFuture<Tuple3<String,Integer, String>> resultFuture) throws Exception {
            List list = new ArrayList<Tuple2<Integer, String>>();
            list.add(new Tuple3<>(input.f0,input.f1, ""));
            resultFuture.complete(list);
        }
    }
}