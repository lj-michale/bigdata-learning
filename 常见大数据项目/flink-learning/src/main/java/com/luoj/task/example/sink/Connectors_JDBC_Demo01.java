//package com.luoj.task.example.sink;
//
//import lombok.AllArgsConstructor;
//import lombok.Data;
//import lombok.NoArgsConstructor;
//import org.apache.flink.api.common.RuntimeExecutionMode;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
//import org.apache.flink.connector.jdbc.JdbcSink;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//
///**
// * @program flink-demo
// * @description: 演示官方提供的jdbcSink
// * @author: erainm
// * @create: 2021/03/04 10:21
// */
//public class Connectors_JDBC_Demo01 {
//    public static void main(String[] args) throws Exception {
//        // 1.env
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
//        // 2.source
//        DataStream<Sink_Demo02.Student> studentDS = env.fromElements(new Sink_Demo02.Student(null, "meidusha", 25));
//        //3.Transformation
//        // 4.sink
//        studentDS.addSink(JdbcSink.sink("INSERT INTO `t_student`(`id`, `name`, `age`) VALUES (null, ?, ?);",
//                (ps,t) -> {
//                    ps.setString(1,t.getName());
//                    ps.setInt(2,t.getAge());
//                },
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withUrl("jdbc:mysql://localhost:3306/flink_stu?useSSL=false")
//                        .withDriverName("com.mysql.jdbc.Driver")
//                        .withUsername("root")
//                        .withPassword("666666")
//                        .build()
//        ));
//
//        // 5.execute
//        env.execute();
//    }
//
//    @Data
//    @NoArgsConstructor
//    @AllArgsConstructor
//    public static class Student {
//        private Integer id;
//        private String name;
//        private Integer age;
//    }
//}