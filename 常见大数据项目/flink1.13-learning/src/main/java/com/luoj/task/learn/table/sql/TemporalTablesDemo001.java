package com.luoj.task.learn.table.sql;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.types.DataType;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.DataTypes.*;

/**
 * @author lj.michale
 * @description
 * 时态表（Temporal Tables） #
 * 时态表（Temporal Table）是一张随时间变化的表 – 在 Flink 中称为动态表，时态表中的每条记录都关联了一个或多个时间段，所有的 Flink 表都是时态的（动态的）。
 * 时态表包含表的一个或多个有版本的表快照，时态表可以是一张跟踪所有变更记录的表（例如数据库表的 changelog，包含多个表快照），也可以是物化所有变更之后的表（例如数据库表，只有最新表快照）。
 * 版本: 时态表可以划分成一系列带版本的表快照集合，表快照中的版本代表了快照中所有记录的有效区间，有效区间的开始时间和结束时间可以通过用户指定，根据时态表是否可以追踪自身的历史版本与否，时态表可以分为 版本表 和 普通表。
 * 版本表: 如果时态表中的记录可以追踪和并访问它的历史版本，这种表我们称之为版本表，来自数据库的 changelog 可以定义成版本表。
 * 普通表: 如果时态表中的记录仅仅可以追踪并和它的最新版本，这种表我们称之为普通表，来自数据库 或 HBase 的表可以定义成普通表。
 * @date 2021-08-11
 */
public class TemporalTablesDemo001 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));
        // 状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint");

        //EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Europe/Berlin"));

        // 提供一个汇率历史记录表静态数据集
        List<Tuple2<String, Long>> ratesHistoryData = new ArrayList<>();
        ratesHistoryData.add(Tuple2.of("US Dollar", 102L));
        ratesHistoryData.add(Tuple2.of("Euro", 114L));
        ratesHistoryData.add(Tuple2.of("Yen", 1L));
        ratesHistoryData.add(Tuple2.of("Euro", 116L));
        ratesHistoryData.add(Tuple2.of("Euro", 119L));

        // 用上面的数据集创建并注册一个示例表
        // 在实际设置中，应使用自己的表替换它
        DataStream<Tuple2<String, Long>> ratesHistoryStream = env.fromCollection(ratesHistoryData);
        Table ratesHistory = tableEnv.fromDataStream(ratesHistoryStream, $("r_currency"), $("r_rate"), $("r_proctime").proctime());

        tableEnv.createTemporaryView("RatesHistory", ratesHistory);
        // 创建和注册时态表函数
        // 指定 "r_proctime" 为时间属性，指定 "r_currency" 为主键
        TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("r_proctime", "r_currency");
        tableEnv.createTemporaryFunction("Rates", rates);

        DataType t = INTERVAL(DAY(), SECOND(3));
        // tell the runtime to not produce or consume java.time.LocalDateTime instances
        // but java.sql.Timestamp
        DataType t1 = DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class);

        // tell the runtime to not produce or consume boxed integer arrays
        // but primitive int arrays
        DataType t2 = DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(int[].class);


        env.execute("TemporalTablesDemo001");
    }

}
