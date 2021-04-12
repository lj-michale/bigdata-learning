package com.luoj.task.learn.tablesql;

/**
 * @author lj.michale
 * @description
 * @date 2021-04-10
 */
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
//flink1.10时候是import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import  org.apache.flink.table.api.*;
import static org.apache.flink.table.api.Expressions.*;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.sql.Types;


/**
  * (1)当 Table 被转换成 DataStream 时（参阅与 DataStream 和 DataSet API 结合）。转换完成后，它就成为一个普通的 DataStream 程序，
  * 并会在调用 StreamExecutionEnvironment.execute() 时被执行。注意 从 1.11 版本开始，sqlUpdate 方法 和 insertInto 方法被废弃，
  * (2)从这两个方法构建的 Table 程序必须通过 StreamTableEnvironment.execute() 方法执行，
  *                        而不能通过 StreamExecutionEnvironment.execute() 方法来执行。
  * */
public class StreamTableApi {

    public static void main(String[] args) throws Exception {

        /**
         * 1 注册环境
         */
        EnvironmentSettings mySetting = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//本案例并行度别多开，会生成奇怪的文件。
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, mySetting);

        /**
         * 2 数据源
         */
        DataStream<String> inputsteam = env .readTextFile("D:\\Workplace\\IdeaProjects\\other_project\\flink-learning\\TableApiAndSql\\src\\main\\resources\\datafile\\temperature.txt");
        // inputsteam.print();


  /*      //对比scala
        val dataStream : DataStream[TheSensor] = inputsteam
                .map( data => {
                val dataArray = data.split(",")
                //TheSensor( dataArray(0),dataArray(1).toString,dataArray(2).toDouble )
                TheSensor( dataArray(0).toString,dataArray(1).toDouble )
        })*/

        DataStream<Sensor> dataStream = inputsteam.map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new Sensor(
//                        String.valueOf(split[0]),
//                        Timestamp.valueOf(split[1]),
//                        Double.valueOf(split[2]),
//                        Double.valueOf(split[3])
                );
            }
        });

        /**
         * 2.2 没啥用的分割，因为没有属性，此时Tuple4,不如实体类
         */
        //        env.socketTextStream("localhost", 9999).flatMap()//官网例子
        DataStream<Tuple4<String, Timestamp,Double,Double>> dataStream1 = env
                .readTextFile("D:\\Workplace\\IdeaProjects\\other_project\\flink-learning\\TableApiAndSql\\src\\main\\resources\\datafile\\temperature.txt")
                .flatMap(new mySplitter())
//                .keyBy(0)
//                .timeWindow(Time.seconds(5))
//                .sum(1)
                ;
        dataStream1.print();

        /**
         * 3 TableAPI处理输入数据
         */
        Table dataTable = tableEnv.fromDataStream(dataStream);


        /**
         *4 转换回数据流，打印输出.
         */
       /*
       scala 写法val resultStream : DataStream[(String,Double)] =resultTable.toAppendStream[(String,Double)];
       java没思路 DataStream<Tuple2<String,Double>> resultStream = tableEnv.toAppendStream(resultTable);
        resultStream.print("Result!");*/


        /**
         * 4.1 转换回数据流，打印输出.
         * 定义到文件系统的连接 ; 定义以CSV进行数据格式化 ; new Schema()定义表结构。演示完记得删除生成的文件，否则会报错。
         */
        // create a schema of output Table
        final Schema myschema = new Schema()
                .field("tab", DataTypes.STRING())
                .field("record_time",DataTypes.TIMESTAMP())
                .field("temperature",DataTypes.DOUBLE())
                .field("humidity",DataTypes.DECIMAL(5,2));

        tableEnv.connect(new FileSystem().path("D:\\Workplace\\IdeaProjects\\other_project\\flink-learning\\TableApiAndSql\\src\\main\\resources\\datafile\\theFilteTemperature.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("tab", DataTypes.STRING())
                        .field("record_time",DataTypes.TIMESTAMP())
                        .field("temperature",DataTypes.DOUBLE())
                        .field("humidity",DataTypes.DECIMAL(5,2))
                )
//                .withSchema(myschema)//也可以将schema在外面创建，但是记得用final
                .inAppendMode() //flink1.11新加
                .createTemporaryTable("outputTable");


     /*   Table resultTable= dataTable
                .filter("tab ==='sensor_1'")
                .select("tab,record_time,temperature,humidity");//flink1.10的版本写法*/
        Table resultTable=tableEnv.from("outputTable")
//                .filter($("tab").isEqual("sensor_1"))
//                .groupBy($("tab"), $("record_time"))
                .select($("tab"),
                        $("record_time"),
                        $("temperature").sum(),
                        $("humidity"));

//        table只有一个打印方法，Schema这里代表这个表的组织架构，
//        root
//                |-- tab: STRING
//                |-- record_time: TIMESTAMP(3)
//                |-- temperature: DOUBLE
//                |-- humidity: DOUBLE
        resultTable.printSchema();

        resultTable.executeInsert("outputTable");//flink1.10的写法是resultTable.insertInto("outputTable");

        /**
         * 5.
         */
        env.execute("Start Table Api for Stream");

    }

    /**
     * @author keguoxin
     * @date 2020-05-10
     * @desc
     */
    public static class mySplitter implements FlatMapFunction<String, Tuple4<String, Timestamp,Double,Double>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple4<String, Timestamp,Double,Double>> out) throws Exception {

            for (String word: sentence.split(",")) {
                int a =0;
                System.out.println("The word is " +word);
                a += 1;
            }

            String[] splitResult = sentence.split(",");
            out.collect(new Tuple4<String, Timestamp,Double,Double>(
                    String.valueOf(splitResult[0]),
                    Timestamp.valueOf(splitResult[1]),
                    Double.valueOf(splitResult[2]),
                    Double.valueOf(splitResult[3])
            ));


        }
    }

    //官网https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/datastream_api.html原生的分割
    //感觉像处理一行，即 a b c d 会被整为 [[a,1],[b,1],[c,1],[d,1]]
    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}