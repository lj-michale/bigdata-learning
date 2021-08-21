package com.aurora.mq.kafka;

import com.aurora.mq.AbsDeserialization;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-21
 */
public class CustomerKafkaConsumer<T> extends FlinkKafkaConsumer<T> {

    private AbsDeserialization<T> valueDeserializer;

    public CustomerKafkaConsumer(String topic, AbsDeserialization<T> valueDeserializer, Properties props) {
        super(topic, valueDeserializer, props);
        this.valueDeserializer=valueDeserializer;
    }

    @Override
    public void run(SourceFunction.SourceContext<T> sourceContext) throws Exception {
        valueDeserializer.setRuntimeContext(getRuntimeContext());
        valueDeserializer.initMetric();
        super.run(sourceContext);
    }

}