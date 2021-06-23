package com.luoj.task.learn.join.example001;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author lj.michale
 * @description
 * @date 2021-06-23
 */
public class CityInfoSchema implements DeserializationSchema<CityInfo> {


    @Override
    public CityInfo deserialize(byte[] message) throws IOException {
        String jsonStr = new String(message, StandardCharsets.UTF_8);
        CityInfo data = JSON.parseObject(jsonStr, new TypeReference<CityInfo>() {});
        return data;
    }

    @Override
    public boolean isEndOfStream(CityInfo nextElement) {
        return false;
    }

    @Override
    public TypeInformation<CityInfo> getProducedType() {
        return TypeInformation.of(new TypeHint<CityInfo>() {
        });
    }
}
