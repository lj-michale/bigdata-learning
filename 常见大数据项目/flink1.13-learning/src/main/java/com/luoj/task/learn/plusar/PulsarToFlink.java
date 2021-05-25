package com.luoj.task.learn.plusar;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.util.serialization.PulsarDeserializationSchema;

import java.util.Properties;

/**
 * @author lj.michale
 * @description
 *   flink消费pulsar中数据
 * @date 2021-05-25
 */

public class PulsarToFlink {

    public static void main(String[] args) throws Exception {

        //创建环境，设置参数
        StreamExecutionEnvironment env =             StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.enableCheckpointing(60*1000*5, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new MemoryStateBackend());
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(0, 3 * 1000));

        //定义pulsar相关参数
        String serviceUrl = "pulsar://localhost:6650";
        String adminUrl = "http://localhost:8080";
        String topic = "persistent://tenant/namespaces/topic";

        //创建pulsar source
        Properties properties = new Properties();
        properties.setProperty("topic", topic);
        properties.setProperty("pulsar.producer.blockIfQueueFull", "true");
        FlinkPulsarSource<String> pulsarSource = new FlinkPulsarSource<>(
                serviceUrl, adminUrl, PulsarDeserializationSchema.valueOnly(new SimpleStringSchema()), properties
        );

        pulsarSource.setStartFromEarliest();

        DataStreamSource<String> pulsarDS = env.addSource(pulsarSource);
        pulsarDS.print();

        env.execute();
    }
}

