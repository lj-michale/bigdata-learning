#!/user/bin/env python
# -*- coding: utf-8 -*-

import os

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic, CheckpointingMode, FsStateBackend, ExternalizedCheckpointCleanup, CheckpointConfi
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.types import DataTypes
from imos_ddl import source_kafka_tbl_face_image_record, sink_pgsql_tbl_face_image_record, source_test, sink_test

from pyflink.table.udf import udf

add = udf(lambda i, j: i + j, [DataTypes.BIGINT(), DataTypes.BIGINT()], DataTypes.BIGINT())

# 创建Table Environment， 并选择使用的Planner
env = StreamExecutionEnvironment.get_execution_environment()
config = env.get_checkpoint_config()

env.enable_checkpointing(5000, CheckpointingMode.EXACTLY_ONCE)
# env.get_checkpoint_config(config)
env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

#checkPointPath = FileSystem.path("file:///home/flink/cdn_daemon_checkpoints")
stateBackend = FsStateBackend("file:///var/flink/face_image/")
env.set_state_backend(stateBackend)

config.enable_externalized_checkpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


t_env = StreamTableEnvironment.create(
   env,
   environment_settings=EnvironmentSettings.new_instance().use_blink_planner().build())


# 创建Kafka数据源表
res1=t_env.sql_update(source_test)
res2=t_env.sql_update(sink_test)

t_env.register_function("add", add)

# 添加依赖的Python文件
t_env.add_python_file(
    os.path.dirname(os.path.abspath(__file__)) + "/imos_ddl.py")

t_env.from_path("tbl_face_test_kafka")\
   .select(" add(id_num, id_num) as id") \
   .insert_into("tbl_face_test")

# 执行作业
t_env.execute("face_image_log")











