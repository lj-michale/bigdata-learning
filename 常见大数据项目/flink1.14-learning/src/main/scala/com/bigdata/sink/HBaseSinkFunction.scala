//package com.bigdata.sink
//
//import com.alibaba.fastjson.{JSON, JSONObject}
//import com.bigdata.bean.OrderObj
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
//import org.apache.hadoop.conf
//import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
//import org.apache.hadoop.hbase.client._
//
///**
// * @author lj.michale
// * @description  OrderObj Sink to HBase
// * @date 2021-08-21
// */
//class HBaseSinkFunction extends RichSinkFunction[OrderObj]{
//
//  var connection:Connection = _
//  var hbTable:Table  =  _
//
//  override def open(parameters: Configuration): Unit = {
//    val configuration: conf.Configuration = HBaseConfiguration.create()
//    configuration.set("hbase.zookeeper.quorum", "node01,node02,node03")
//    configuration.set("hbase.zookeeper.property.clientPort", "2181")
//    connection = ConnectionFactory.createConnection(configuration)
//    hbTable = connection.getTable(TableName.valueOf("flink:data_orders"))
//  }
//
//  override def close(): Unit = {
//    if(null != hbTable){
//      hbTable.close()
//    }
//    if(null != connection){
//      connection.close()
//    }
//
//
//  }
//
//  def insertHBase(hbTable: Table, orderObj: OrderObj) = {
//
//    val database: String = orderObj.database
//    val table: String = orderObj.table
//    val value: String = orderObj.`type`
//    val orderJson: JSONObject = JSON.parseObject(orderObj.data)
//
//    val orderId: String = orderJson.get("orderId").toString
//    val orderNo: String = orderJson.get("orderNo").toString
//    val userId: String = orderJson.get("userId").toString
//    val goodId: String = orderJson.get("goodId").toString
//    val goodsMoney: String = orderJson.get("goodsMoney").toString
//    val realTotalMoney: String = orderJson.get("realTotalMoney").toString
//    val payFrom: String = orderJson.get("payFrom").toString
//    val province: String = orderJson.get("province").toString
//    val createTime: String = orderJson.get("createTime").toString
//
//    val put = new Put(orderId.getBytes())
//    put.addColumn("f1".getBytes(),"orderNo".getBytes(),orderNo.getBytes())
//    put.addColumn("f1".getBytes(),"userId".getBytes(),userId.getBytes())
//    put.addColumn("f1".getBytes(),"goodId".getBytes(),goodId.getBytes())
//    put.addColumn("f1".getBytes(),"goodsMoney".getBytes(),goodsMoney.getBytes())
//    put.addColumn("f1".getBytes(),"realTotalMoney".getBytes(),realTotalMoney.getBytes())
//    put.addColumn("f1".getBytes(),"payFrom".getBytes(),payFrom.getBytes())
//    put.addColumn("f1".getBytes(),"province".getBytes(),province.getBytes())
//    put.addColumn("f1".getBytes(),"createTime".getBytes(),createTime.getBytes())
//    hbTable.put(put)
//
//  }
//
//  def deleteHBaseData(hbTable: Table, orderObj: OrderObj) = {
//    val orderJson: JSONObject = JSON.parseObject(orderObj.data)
//    val orderId: String = orderJson.get("orderId").toString
//    val delete = new Delete(orderId.getBytes())
//    hbTable.delete(delete)
//  }
//
////  def invoke(orderObj: OrderObj, context: SinkFunction.Context[_]): Unit = {
////    val database: String = orderObj.database
////    val table: String = orderObj.table
////    val typeResult: String = orderObj.`type`
////    if(database.equalsIgnoreCase("product")  &&  table.equalsIgnoreCase("kaikeba_orders")) {
////      if(typeResult.equalsIgnoreCase("insert")) {
////        //插入hbase数据
////        insertHBase(hbTable,orderObj)
////      } else if(typeResult.equalsIgnoreCase("update")) {
////        //更新hbase数据
////        insertHBase(hbTable,orderObj)
////
////      } else if(typeResult.equalsIgnoreCase("delete")) {
////        //删除hbase数据
////        deleteHBaseData(hbTable,orderObj)
////      }
////    }
////  }
//
//
//}
//
