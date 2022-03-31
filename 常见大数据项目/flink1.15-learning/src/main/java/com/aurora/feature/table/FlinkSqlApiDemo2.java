package com.aurora.feature.table;

import com.aurora.generate.WordCountSource1ps;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @descri 使用SQL实现流批一体
 * 由于SQL是Table的更高层封装，更适合不需要关心平台组件底层实现的业务开发者【也就是俗称的SQL Boy】使用，既然Table层面已经实现了流批一体，那么SQL层面必然也可以实现。
 *
 * @author lj.michale
 * @date 2022-04-01
 */
public class FlinkSqlApiDemo2 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> data1 = env.addSource(new WordCountSource1ps());
        String inputPath = "E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.15-learning\\data\\output\\output1.txt";
        DataStreamSource<String> data2 = env.readTextFile(inputPath);

        Table streamTable = streamTableEnv.fromDataStream(data1);
        Table batchTable = streamTableEnv.fromDataStream(data2);

        Table streamTable1 = streamTable.where($("f0").like("%2%"));
        Table batchTable1 = batchTable.where($("f0").like("%2%"));

        Table t1 = streamTableEnv.sqlQuery("SeLeCt UPPER(f0) frOm " + streamTable1);
        Table t2 = streamTableEnv.sqlQuery("SeLeCt UPPER(f0) frOm " + batchTable1);

        DataStream<Row> s1 = streamTableEnv.toDataStream(t1);
        DataStream<Row> s2 = streamTableEnv.toDataStream(t2);

        s1.print();
        s2.print();

        env.execute();
    }

}
