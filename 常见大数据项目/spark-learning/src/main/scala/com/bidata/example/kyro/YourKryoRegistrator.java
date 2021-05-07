package com.bidata.example.kyro;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-05
 */
public class YourKryoRegistrator implements KryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {
        // 在Kyro序列化库中注册自定义的类
        kryo.register(YourClass.class, new FieldSerializer(kryo, YourClass.class));
    }

}
