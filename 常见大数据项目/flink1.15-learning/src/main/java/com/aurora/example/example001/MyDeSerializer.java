package com.aurora.example.example001;

import com.aurora.bean.Jason;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author lj.michale
 * @description
 * @date 2022-04-02
 */
public class MyDeSerializer  implements KafkaDeserializationSchema<Jason> {

    @Override
    public boolean isEndOfStream(Jason jason) {
        return false;
    }

    @Override
    public Jason deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        return null;
    }

    @Override
    public TypeInformation<Jason> getProducedType() {
        return null;
    }
}
