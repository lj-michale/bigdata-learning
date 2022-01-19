package com.luoj.example.example1;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;


public class MyDeserializationSchema implements DebeziumDeserializationSchema<String>{

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> out) throws Exception {
        JSONObject resultObj = new JSONObject();
        Struct valueStruct = (Struct) sourceRecord.value();

        Struct sourceStruct = valueStruct.getStruct("source");

        // 获取数据库名
        String database = sourceStruct.getString("db");
        // 获取表名
        String table = sourceStruct.getString("table");
        // 获取操作类型
        String type = Envelope.operationFor(sourceRecord).toString().toLowerCase();

        // 修复操作
        if("create".equals(type)){
            type = "insert";
        }

        // 定义一个json对象，用于封装影响的记录 Struct => JSONObject
        Struct afterStruct = valueStruct.getStruct("after");
        JSONObject dataJsonObj = new JSONObject();
        if(afterStruct!=null){
            // 获取所有的字段
            List<Field> fieldList = afterStruct.schema().fields();
            // 对每个字段进行操作
            for (Field field : fieldList) {
                String fieldName = field.name();
                Object fieldValue = afterStruct.get(field);
                dataJsonObj.put(fieldName,fieldValue);
            }
        }

        // 封装对象
        resultObj.put("database",database);
        resultObj.put("table",table);
        resultObj.put("type",type);
        resultObj.put("data",dataJsonObj);

        out.collect(resultObj.toJSONString());
    }
    // 作用？
    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}