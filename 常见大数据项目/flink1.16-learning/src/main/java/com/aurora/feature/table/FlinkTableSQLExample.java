package com.aurora.feature.table;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

/**
 * @descri FlinkSQL流式关联Hbase大表方案(走二级索引)
 *         https://blog.csdn.net/qq_32068809/article/details/122862330
 *
 * @author lj.michale
 * @date 2022-05-19
 */
public class FlinkTableSQLExample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 接入socket流,测试数据如下
        // {'name':'kafka_tb','type':'INSERT','new':{'id':'1','name':'lxz'}}
//        DataStream<String> dataStream = env.socketTextStream("localhost", 9999);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("p88-dataplat-slave1:9092,p88-dataplat-slave2:9092,p88-dataplat-slave3:9092")
                .setTopics("blue")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 定义kafka_tb表类型（有序）
        TypeInformation[] kafka_tb_types = new TypeInformation[]{Types.STRING, Types.STRING};
        RowTypeInfo kafka_tb_rowType = new RowTypeInfo(kafka_tb_types);

        // kafka接收到的流转换后注册成kafka_tb表
        DataStream<Row> ds = dataStream.flatMap(new FlatMapFunction<String, Row>() {
            @Override
            public void flatMap(String value, Collector<Row> out) throws Exception {
                String type = JSON.parseObject(value).getString("type");
                JSONObject new_row = JSON.parseObject(value).getJSONObject("new");
                switch (type) {
                    case "INSERT":
                        out.collect(Row.ofKind(RowKind.INSERT, new_row.getString("id"), new_row.getString("name")));break;
                }
            }
        }).returns(kafka_tb_rowType);

        // 注册kafka表kafka_tb
        Schema schema01 = Schema.newBuilder().build();
        Table tab1 = tEnv.fromChangelogStream(ds,schema01).as("id","name");
        tEnv.createTemporaryView("kafka_tb", tab1);

        ds.print();
/*
        // 注册Hbase索引表hbase_index_tb
        tEnv.executeSql("CREATE TABLE hbase_index_tb (\n" +
                " ID STRING,\n" +
                " CF ROW<NAME STRING>,\n" +
                " PRIMARY KEY (ID) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'hbase_index_tb',\n" +
                " 'zookeeper.quorum' = 'prod-bigdata-pc10:2181,prod-bigdata-pc14:2181,prod-bigdata-pc15:2181',\n" +
                " 'zookeeper.znode.parent' = '/hbase-unsecure'\n"+
                ")");

        // 注册Hbase数据表hbase_data_tb
        tEnv.executeSql("CREATE TABLE hbase_data_tb (\n" +
                " ID STRING,\n" +
                " CF ROW<CITY STRING,AGE INT,SEX STRING,NAME STRING,GRADE FLOAT>,\n" +
                " PRIMARY KEY (ID) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'hbase_data_tb',\n" +
                " 'zookeeper.quorum' = 'prod-bigdata-pc10:2181,prod-bigdata-pc14:2181,prod-bigdata-pc15:2181',\n" +
                " 'zookeeper.znode.parent' = '/hbase-unsecure'\n"+
                ")");

        // 执行关联查询
        tEnv.executeSql("select a.* " +
                "from hbase_data_tb a " +
                "join hbase_index_tb b " +
                "on a.ID = b.ID " +
                "join kafka_tb c " +
                "on  c.name=b.NAME").print();
*/

    }

}
