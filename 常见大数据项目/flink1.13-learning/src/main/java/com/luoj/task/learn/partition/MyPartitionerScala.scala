package com.luoj.task.learn.partition

import org.apache.flink.api.common.functions.Partitioner

class MyPartitionerScala extends Partitioner[Long]{
  override def partition(key: Long, numPartitions: Int) = {
    println("分区总数："+numPartitions)
    if(key % 2 ==0){
      0
    }else{
      1
    }
  }
}