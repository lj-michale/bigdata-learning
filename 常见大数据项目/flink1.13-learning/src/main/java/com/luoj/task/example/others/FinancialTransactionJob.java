package com.luoj.task.example.others;

import com.luoj.task.example.pv.MySensorSource;
import com.luoj.task.example.pv.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-27
 */
public class FinancialTransactionJob {

    private final static int FILE_SIZE_THESHOLD = 300;

    public static void main(String[] args) throws Exception {


        // Set parameters
        ParameterTool parameters = ParameterTool.fromArgs (args);
        String inputTopic = parameters.get ("inputTopic", "transactions");
        String outputTopic = parameters.get ("outputTopic", "fraud");
        String kafka_host = parameters.get ("kafka_host", "broker.kafka.l4lb.thisdcos.directory:9092");

        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment ();

        // Checkpoint
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("hdfs:///checkpoints-data/");
//        env.getCheckpointConfig().setCheckpointStorage(
//                new FileSystemCheckpointStorage("hdfs:///checkpoints-data/", FILE_SIZE_THESHOLD ));

        // state_backends状态后端 https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/ops/state/state_backends/
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir");

        // If you manually passed FsStateBackend into the RocksDBStateBackend constructor
        // to specify advanced checkpointing configurations such as write buffer size,
        // you can achieve the same results by using manually instantiating a FileSystemCheckpointStorage object.
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoint-dir"));
        env.setParallelism(1);
        // 缓冲区
        env.setBufferTimeout(50L);
        // 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.of(30, TimeUnit.SECONDS)));

        Properties properties = new Properties();
        properties.setProperty ("bootstrap.servers", kafka_host);
        properties.setProperty ("group.id", "flink_consumer");

        DataStreamSource<SensorReading> stream = env.addSource(new MySensorSource());

        SingleOutputStreamOperator<String> strem2 = stream.uid("source-id").shuffle().map(new MapFunction<SensorReading, String>() {
            @Override
            public String map(SensorReading sensorReading) throws Exception {
                String str = sensorReading.id() + sensorReading.timestamp() + sensorReading.temperature();
                return str;
            }
        }).uid("mapper-id");

        strem2.print();

        env.execute("FinancialTransactionJob");
    }

}
