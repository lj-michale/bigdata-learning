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
        kryo.register(YourClass.class, new FieldSerializer(kryo, YourClass.class));
    }

}
