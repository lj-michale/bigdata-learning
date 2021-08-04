package com.luoj.task.learn.datagenerator;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-03
 */
import com.luoj.bean.TrafficData;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;

public class TestSource {

    public static void main(String[] args) throws Exception {
        // 创建env对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建DataGeneratorSource。传入上面自定义的数据生成器
        DataGeneratorSource<TrafficData> trafficDataDataGeneratorSource
                = new DataGeneratorSource<>(new TrafficData.TrafficDataGenerator());

        // 添加source
        env.addSource(trafficDataDataGeneratorSource)
                // 指定返回类型
                .returns(new TypeHint<TrafficData>() {
                }).print();
        env.execute();
    }
}

