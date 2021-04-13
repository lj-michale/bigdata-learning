#-*- encoding: utf-8 -*-
'''
flink_kafka_demo.py
Created on 2020/8/1 15:19
'''
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings,TableEnvironment,TableSource
from pyflink.table.descriptors import Schema, Kafka, Json, Rowtime,FileSystem,OldCsv
from pyflink.dataset import *

s_env = StreamExecutionEnvironment.get_execution_environment()
s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
s_env.set_parallelism(1)

# use blink table planner
env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_streaming_mode().build()

st_env =  StreamTableEnvironment.create(s_env, environment_settings=env_settings)
st_env.get_config().get_configuration().set_string("pipeline.jars", "file:///root/anaconda3/lib/python3.6/site-packages/pyflink/lib/flink-connector-kafka_2.11-1.11.0.jar;file:///root/anaconda3/lib/python3.6/site-packages/pyflink/lib/flink-connector-kafka-base_2.11-1.11.0.jar;file:///root/anaconda3/lib/python3.6/site-packages/pyflink/lib/flink-jdbc_2.11-1.11.0.jar;file:///root/anaconda3/lib/python3.6/site-packages/pyflink/lib/flink-sql-connector-kafka_2.11-1.11.0.jar;file:///root/anaconda3/lib/python3.6/site-packages/pyflink/lib/kafka-clients-2.1.0.jar")


#读kafka
properties = {
    "zookeeper.connect" : "nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181", 
    "bootstrap.servers" : "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092", 
    "group.id" : "testGroup"
    }
st_env.connect(Kafka().properties(properties).version("universal").topic("test").start_from_latest()) \
    .with_format(Json()).with_schema(Schema() \
        .field('throughputReqMax', DataTypes.BIGINT()) \
        .field('throughputReqTotal', DataTypes.BIGINT())) \
    .create_temporary_table('mySource')

#写入csv
st_env.connect(FileSystem().path('/usr/local/flink/test/result3.txt')) \
    .with_format(OldCsv()
                .field('sub', DataTypes.BIGINT())) \
    .with_schema(Schema()
                 .field('sub', DataTypes.BIGINT())) \
    .create_temporary_table('mySink')

#读取kafka数据中的a和b字段相加再乘以2 , 并插入sink
st_env.from_path('mySource')\
    .select("(throughputReqTotal-throughputReqMax)") \
    .insert_into('mySink')
st_env.execute("job_test")
