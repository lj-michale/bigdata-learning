package com.luoj.task.example.finance;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author lj.michale
 * @description  基于Flink1.13
 * @date 2021-05-27
 */
@Slf4j
public class FinancialTransactionJob {

    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromArgs (args);
        String inputTopic = parameters.get ("inputTopic", "transactions");
        String outputTopic = parameters.get ("outputTopic", "fraud");
        String kafka_host = parameters.get ("kafka_host", "broker.kafka.l4lb.thisdcos.directory:9092");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment ();

        Properties properties = new Properties();
        properties.setProperty ("bootstrap.servers", kafka_host);
        properties.setProperty ("group.id", "flink_consumer");


        DataStream<Transaction> transactionsStream = env
                .addSource (new FlinkKafkaConsumer010<>(inputTopic, new SimpleStringSchema(), properties))
                // Map from String from Kafka Stream to Transaction.
                .map (new MapFunction<String, Transaction>() {
                    @Override
                    public Transaction map(String value) throws Exception {
                        return new Transaction (value);
                    }
                });


        SingleOutputStreamOperator<Transaction> timestampedStream = transactionsStream.assignTimestampsAndWatermarks(new MyWaterMark());
        timestampedStream.print ();

        env.execute("FinancialTransactionJob");
    }


    //  todo 功能是统计窗口中的条数，即遇到一条数据就加一
    public static class CountAgg implements AggregateFunction<JSONObject, Long, Long> {
        @Override
        public Long createAccumulator() { //创建一个数据统计的容器，提供给后续操作使用。
            return 0L;
        }

        @Override
        public Long add(JSONObject json, Long acc) { //每个元素被添加进窗口的时候调用。
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            //窗口统计事件触发时调用来返回出统计的结果。
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) { //只有在当窗口合并的时候调用,合并2个容器
            return acc1 + acc2;
        }
    }

    // todo 指定格式输出
    public static class WindowResultFunction implements WindowFunction<Long, JSONObject, String, TimeWindow> {
        @Override
        public void apply(
                String key,  // 窗口的主键，即 itemId
                TimeWindow window,  // 窗口
                Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
                Collector<JSONObject> collector  // 输出类型为 ItemViewCount
        ) throws Exception {
            Long count = aggregateResult.iterator().next();
            //窗口结束时间
            long end = window.getEnd();
            JSONObject json = new JSONObject();
            json.put("key",key);
            json.put("count",count);
            json.put("end",end);
            collector.collect(json);
        }
    }

}
