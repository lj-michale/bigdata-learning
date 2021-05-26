package com.luoj.task.learn.matric.example001;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import java.util.Properties;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-26
 */
public class CustomerKafkaConsumer<T> extends FlinkKafkaConsumer010<T> {

    private AbsDeserialization<T> valueDeserializer;

    public CustomerKafkaConsumer(String topic, AbsDeserialization<T> valueDeserializer, Properties props) {
        super(topic, valueDeserializer, props);
        this.valueDeserializer=valueDeserializer;
    }

    @Override public void run(SourceContext<T> sourceContext) throws Exception {
        valueDeserializer.setRuntimeContext(getRuntimeContext());
        valueDeserializer.initMetric();
        super.run(sourceContext);
    }
}
