package com.bidata.example.udf.example001

import java.util
import org.apache.hadoop.hive.ql.exec.{UDFArgumentException, UDFArgumentLengthException}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

class MyUdtf extends GenericUDTF{

  //  这个方法的作用 ：1.输入参数校验  2. 输出列定义，可以多于 1 列，相当于可以生成多行多列数据
  override def initialize(args:Array[ObjectInspector]): StructObjectInspector = {
    //  我们这里，值接收一个参数
    if (args.length != 1) {
      throw new UDFArgumentLengthException("UserDefinedUDTF takes only one argument")
    }
    //  我们这里只接收基本类型
    if (args(0).getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentException("UserDefinedUDTF takes string as a parameter")
    }
    //  属性名集合
    val fieldNames = new util.ArrayList[String]
    //  属性 oi 集合
    val fieldOIs = new util.ArrayList[ObjectInspector]

    //  这里定义的是输出列默认字段名称
    fieldNames.add("col1")
    //  这里定义的是输出列字段类型 ( String )
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    //  封装 ( 列名，列类型 )
    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs)
  }

  //  这是处理数据的方法，入参数组里只有 1 行数据,即每次调用 process 方法只处理一行数据
  override def process(args: Array[AnyRef]): Unit = {
    //  将字符串切分成单个字符的数组 ( 用空格切分 )
    val strLst: Array[String] = args(0).toString.split(" ")
    for(i <- strLst){
      var tmp:Array[String] = new Array[String](1)
      tmp(0) = i
      //  输出：调用forward方法，必须传字符串数组，即使只有一个元素
      forward(tmp)
    }
  }

  //  释放资源
  override def close(): Unit = {}

}
