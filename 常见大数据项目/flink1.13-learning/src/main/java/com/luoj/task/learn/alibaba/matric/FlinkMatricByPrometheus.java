package com.luoj.task.learn.alibaba.matric;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author lj.michale
 * @description  Metric打点
 * @date 2021-12-21
 */
@Slf4j
public class FlinkMatricByPrometheus {

    public static void main(String[] args) throws Exception {

        // datagen
        String sourceDDL = "CREATE TABLE gen_source (\n" +
                " event INT \n" +
                " ) WITH ( \n" +
                " 'connector' = 'datagen'\n" +
                " )";

        // Sink to Print
        String sinkDDL = "CREATE TABLE print_sink (\n" +
                " event INT \n" +
                " ) WITH (\n" +
                " 'connector' = 'print'\n" +
                " )";

        // 创建执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(1, TimeUnit.SECONDS) ));
        sEnv.enableCheckpointing(1000);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, settings);

        //注册source和sink
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        tEnv.createFunction("myudf", MyUDFWithMetric.class);

        //数据提取
        Table sourceTab = tEnv.from("gen_source");
        //这里我们暂时先使用 标注了 deprecated 的API, 因为新的异步提交测试有待改进...
        sourceTab.select(call("myudf", $("event"))).insertInto("print_sink");

        //执行作业
        tEnv.execute("flink-prometheus");

    }


}
