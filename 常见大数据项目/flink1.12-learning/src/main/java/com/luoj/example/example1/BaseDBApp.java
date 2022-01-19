package com.luoj.example.example1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 基本环境处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 2 检查点设置

        // 1 开启检查点 保存周期为5秒，检查点类型为精准一次
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2 设置最大超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        // 3 设置取消job之后，检查点是否保留  保留撤销
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 4 设置重启策略  固定延迟重启，重启次数3秒，间隔3秒才重新启动
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        // 5 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/flink/ck"));
        // 6 设置操作启动用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 3 创建消费者
        String topic = "ods_base_db";
        String groupId = "base_db_app_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 4 ETL 主流
        SingleOutputStreamOperator<JSONObject> masterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return jsonObject.getString("table") != null &&
                                jsonObject.getString("table").length() > 0 &&
                                jsonObject.getJSONObject("data") != null &&
                                jsonObject.getString("data").length() > 3;

                    }
                }
        );
        //masterDS.print("主流打印");

        //TODO 5 使用FlinkCDC读取配置表数据--封装成流 从流

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall2022_realtime")
                .tableList("gmall2022_realtime.table_process")
                .username("root")
                .password("000000")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserializationSchema())
                .build();

        // 先全表扫描
        DataStreamSource<String> followDS = env.addSource(sourceFunction);

        // 打印followDS
        //followDS.print("配置表：");

        //TODO 6 配置广播--定义广播变量
        MapStateDescriptor<String, TableProcess> tableList = new MapStateDescriptor<>(
                "tableList",
                String.class,
                TableProcess.class
        );
        BroadcastStream<String> broadcastDS = followDS.broadcast(tableList);



        //TODO 7 使用connect连接在一起
        BroadcastConnectedStream<JSONObject, String> resultDS = masterDS.connect(broadcastDS);


        //TODO 8 动态分流，分别处理两条流的数据

        // 创建侧输出流标签
        OutputTag<JSONObject> dimTag = new OutputTag<JSONObject>("dimTag") {};
        // 获取主流数据
        SingleOutputStreamOperator<JSONObject> realDS = resultDS.process(new TableProcessFunction(dimTag, tableList));
        // 获取从流数据
        DataStream<JSONObject> dimDS = realDS.getSideOutput(dimTag);

        realDS.print("主流");
        dimDS.print("从流");

        //TODO 9 将维度侧输出流的数据写到phoenix维度表中
        dimDS.addSink(new DimSink());
        //TODO 10 将主流数据写到kafka中
        realDS.addSink(
                // 调用工具类的方法  传入序列化器
                MyKafkaUtil.getKafkaSinkBySchema(
                        new KafkaSerializationSchema<JSONObject>() {
                            // producerRecord这个对象是生产者包装的数据
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                                String topic = jsonObject.getString("sink_table");
                                return new ProducerRecord<byte[], byte[]>(topic,jsonObject.getJSONObject("data").toJSONString().getBytes());
                            }
                        }
                )
        );
        //TODO 执行
        env.execute();
    }
}