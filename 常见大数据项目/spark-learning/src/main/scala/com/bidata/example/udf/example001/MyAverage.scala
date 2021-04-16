package com.bidata.example.udf.example001

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


class MyAverage extends UserDefinedAggregateFunction  {
  //  思路 ：
  //      1 ，数据 ： 分片的
  //      2 ，平均值 ： 需要所有数据
  //      3 ，缓存 ： 把所有的数据加起来
  //      4 ，初始值 ： 0
  //      5 ，merge ： 所有分片的值加起来
  //  =================================
  //  聚合函数输入参数的数据类型 ( 求平均工资，double 类型，集合 )
  //  聚合函数输入参数的数据类型 ( 求平均工资，double 类型，集合 )
  override def inputSchema: StructType = StructType(StructField("salary",DoubleType)::Nil)
  //  缓存数据的数据类型 ( 二元素集合：总工资，人数 )
  override def bufferSchema: StructType =
    StructType(StructField("salary",DoubleType)::StructField("count",LongType)::Nil)
  //  返回值的数据类型
  override def dataType: DataType = DoubleType
  //  幂等性：对于相同的输入是否一直返回相同的输出。
  override def deterministic: Boolean = true
  //  初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0.0
    buffer(1)=0L
  }
  //  相同 Execute 间的数据合并。
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) + input.getDouble(0)
    buffer(1) = buffer.getLong(1)+ 1
  }
  //  不同 Execute 间的数据合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }
  //  计算最终结果
  override def evaluate(buffer: Row): Double= {
    buffer.getDouble(0) / buffer.getLong(1)
  }

}
