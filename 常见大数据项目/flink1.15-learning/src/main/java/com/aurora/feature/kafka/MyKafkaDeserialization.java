package com.aurora.feature.kafka;

import com.alibaba.fastjson.JSON;
import com.aurora.bean.Jason;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;

/**
 * @descri 自定义序列化器
 *
 * @author lj.michale
 * @date 2022-04-14
 */
public class MyKafkaDeserialization implements KafkaDeserializationSchema<Jason> {

    private static final Logger log = Logger.getLogger(MyKafkaDeserialization.class);
    private final String encoding = "UTF8";
    private boolean includeTopic;
    private boolean includeTimestamp;

    public MyKafkaDeserialization(boolean includeTopic, boolean includeTimestamp) {
        this.includeTopic = includeTopic;
        this.includeTimestamp = includeTimestamp;
    }

    @Override
    public TypeInformation<Jason> getProducedType() {
        return TypeInformation.of(Jason.class);
    }

    @Override
    public boolean isEndOfStream(Jason nextElement) {
        return false;
    }

    @Override
    public Jason deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        if (consumerRecord != null) {
            try {
                String value = new String(consumerRecord.value(), encoding);
                Jason jason = JSON.parseObject(value, Jason.class);
                if (includeTopic) {
                    jason.setTopic(consumerRecord.topic());
                }
                if (includeTimestamp) {
                    jason.setTimestamp(consumerRecord.timestamp());
                }
                return jason;
            } catch (Exception e) {
                log.error("deserialize failed : " + e.getMessage());
            }
        }
        return null;
    }
}