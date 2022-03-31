package com.aurora.feature.datastream;

import com.aurora.generate.WordCountSource1ps;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @descri 使用DataStream实现流批一体
 *
 * @author lj.michale
 * @date 2022-04-01
 */
public class FlinkSqlApiDemo1 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> data1 = env.addSource(new WordCountSource1ps());

        String inputPath = "E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.15-learning\\data\\output\\output1.txt";
        DataStreamSource<String> data2 = env.readTextFile(inputPath);

        data1.print("data1");
        data2.print("data2");

        env.execute();
    }

}
