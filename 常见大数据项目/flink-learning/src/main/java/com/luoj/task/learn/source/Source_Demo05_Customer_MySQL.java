package com.luoj.task.learn.source;

import com.luoj.task.learn.partition.MyPartition;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

/**
 * @program flink-demo
 * @description: 演示DataStream-Source-基于自定义数据源-MYSQL
 * @author: erainm
 * @create: 2021/03/03 15:17
 */
public class Source_Demo05_Customer_MySQL {

    public static void main(String[] args) throws Exception {
        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 2.source
        DataStreamSource<Student> streamSource = env.addSource(new MySQLSource()).setParallelism(2);
        // 自定义分区
        streamSource.partitionCustom(new MyPartition(), 0);
        // 3.transformation

        // 4.sink
        streamSource.print();
        // 5.execute
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }

    public static class MySQLSource extends RichParallelSourceFunction<Student>{

        private Connection conn = null;
        private PreparedStatement ps = null;
        private ResultSet rs = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            //加载驱动,开启连接
            //Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/jiguang?useSSL=true&serverTimezone=UTC", "root", "abc1314520");
            String sql = "select id,name,age from t_student";
            ps = conn.prepareStatement(sql);
        }
        private boolean flag = true;

        @Override
        public void run(SourceContext<Student> ctx) throws Exception {
            while (flag) {
                rs = ps.executeQuery();
                while (rs.next()) {
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    int age = rs.getInt("age");
                    ctx.collect(new Student(id, name, age));
                }
                TimeUnit.SECONDS.sleep(5);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }

        @Override
        public void close() throws Exception {
            if (conn != null) {
                conn.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (rs != null) {
                rs.close();
            }
        }
    }
}