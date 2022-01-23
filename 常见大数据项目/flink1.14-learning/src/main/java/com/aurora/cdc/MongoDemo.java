//package com.aurora.cdc;
//
//import com.ververica.cdc.connectors.mongodb.MongoDBSource;
//import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//
///**
// * @desc :
// */
//@Slf4j
//public class MongoDemo {
//    public static void main(String[] args) throws Exception {
//        SourceFunction<String> sourceFunction = MongoDBSource.<String>builder()
//                .hosts("123.57.207.186:32017")
//                .username("root")
//                .password("YHninja3")
//                .database("ninja3")
//                .collection("ninja_usertable")
//                .deserializer(new JsonDebeziumDeserializationSchema())
//                .build();
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.addSource(sourceFunction)
//                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
//
//        env.execute();
//    }
//}