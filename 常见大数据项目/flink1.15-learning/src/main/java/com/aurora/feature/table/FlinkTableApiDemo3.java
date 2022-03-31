package com.aurora.feature.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @descri 使用TableAPI实现流批一体
 *
 * @author lj.michale
 * @date 2022-04-01
 */
public class FlinkTableApiDemo3 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        String[] str1 = {"hehe1", "haha1", "哈哈1", "哈哈1"};

        Table table1 = streamTableEnv.fromValues(str1);
        Table table1_1 = table1.where($("f0").like("%h%"));

        DataStream<Row> batchTable1 = streamTableEnv.toDataStream(table1_1);
        batchTable1.print();

        System.out.println("*************************");

        DataStreamSource<String> dataStream2 = env.fromElements(str1);
        Table table2 = streamTableEnv.fromDataStream(dataStream2);
        Table table2_1 = table2.where($("f0").like("%哈%"));
        DataStream<Row> batchTable2 = streamTableEnv.toDataStream(table2_1);
        batchTable2.print();

        env.execute();

    }

}
