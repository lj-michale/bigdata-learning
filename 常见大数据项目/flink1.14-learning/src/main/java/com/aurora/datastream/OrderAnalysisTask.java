package com.aurora.datastream;

import com.aurora.source.GenerateCustomOrderSource;
import lombok.extern.slf4j.Slf4j;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-19
 */
@Slf4j
public class OrderAnalysisTask {

    public static void main(String[] args) throws Exception {

        ParameterTool paramTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 缓冲区
        env.setBufferTimeout(50L);
        // 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.of(30, TimeUnit.SECONDS)));
        // 状态后端-HashMapStateBackend
        env.setStateBackend(new HashMapStateBackend());
        //等价于FsStateBackend
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.14-learning\\checkpoint"));
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<Tuple7<String, String, String, String, String, Double, String>> oredrDS = env.addSource(new GenerateCustomOrderSource());

        // 计算BuyCount指标
        SingleOutputStreamOperator<Tuple8<String, String, String, String, String, Double, Integer, String>> ds = oredrDS.map(new MapFunction<Tuple7<String, String, String, String, String, Double, String>, Tuple8<String, String, String, String, String, Double, Integer, String>>() {
            @Override
            public Tuple8<String, String, String, String, String, Double, Integer, String> map(Tuple7<String, String, String, String, String, Double, String> input) throws Exception {
                Integer buyCount = (int) (input.f5 / Double.valueOf(input.f4.toString()));
                return new Tuple8<>(input.f0, input.f1, input.f2, input.f3, input.f4, input.f5, buyCount, input.f6);
            }
        }).name("addBuyCount");

        // 添加水印






        ds.print();




        env.execute("OrderAnalysisTask");

    }


    public static long pare(String time){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        long s = 0;
        try {
            s = sdf.parse(time).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return s;
    }

}
