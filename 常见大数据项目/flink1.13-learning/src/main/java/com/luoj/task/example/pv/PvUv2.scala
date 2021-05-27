package com.luoj.task.example.pv

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.rocksdb.RocksDB
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

/**
 * @descr PV、UV计算
 * @author lj.michale
 * @date 2021/5/27 14:02
 */
object PvUv2 {

  def main(args: Array[String]): Unit = {

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // Checkpoint
    // checkpoint 保留策略,https://blog.csdn.net/weixin_30533797/article/details/97013222
    val config = env.getCheckpointConfig
        config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("files:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoints")




    env.execute(this.getClass.getName)


  }





}
