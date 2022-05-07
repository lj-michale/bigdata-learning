//package com.aurora
//
//import java.sql.{DriverManager, ResultSet}
//import scala.reflect.classTag
//import scala.reflect.runtime.universe._
//
//object  PhoenixUtil {
//
//  //加载配置信息：
//  val driverName = "org.apache.phoenix.jdbc.PhoenixDriver"
//
//  //我的测试集群分别为test001,test002,test003
//  //phoenix对应的测试库为hbase
//  val phoenixUrl = "jdbc:phoenix:test001,test002,test003:2181:/hbase;autocommit=true"
//  Class.forName(driverName)
//
//  //创建连接
//  val conn = DriverManager.getConnection(phoenixUrl)
//  val statement = conn.createStatement()
//
//  //声明一个查询Phoenix的工具类
//  //传入的参数是泛型(这里准备的是样例类)和sql
//  def getQueryResult[T](querySql:String)(implicit m: Manifest[T]):Option[T] = {
//    try{
//      //执行查询语句
//      val result:ResultSet = statement.executeQuery(querySql)
//      while (result.next()){
//        val recordClass = classTag[T].runtimeClass
//        val record:T = recordClass.getConstructor().newInstance().asInstanceOf[T]
//        typeOf[T].members.withFilter(!_.isMethod).map(row=>{
//          //          println(result.getString(row.name.toString.trim))
//          val method = recordClass.getMethod(s"set${row.name.toString.trim.capitalize}",classOf[String])
//          method.invoke(record,result.getString(row.name.toString.trim))
//        })
//        return Some(record)
//      }
//      result.close()
//      return None
//    }catch {
//      case ex:Exception=>ex.printStackTrace()
////        release()
//        return None
//    }
//  }
//
////  def main(args: Array[String]): Unit = {
////    //测试sql
////    val selectSql = s"""select "book_id","book_name" from "phoenix_test_bookName0209" where "ROW" = '${book_id}' """
////
////    //println(getQueryResult[LandpageBookRecord](selectSql ))
////    //这里传入的是 LandpageBookRecord 样例类
////    //此时返回的value值是被some() 所包裹着(整条样例类数据)
////    //如果想去掉some()的包裹，可以用value.getOrElse() 去掉some()
////    val value = getQueryResult[LandpageBookRecord](selectSql )
////
////    //个人业务需要，需要将获取到样例类个别字段数据
////    //用到模式匹配
////    val landPvUvRecord =
////    value match {
////      //如果不为空的话，可以通过lbRecord打点的方式进行值获取
////      //(lbRecord自定义变量),为空则用(无业务逻辑的)默认值填充
////      case Some(lbRecord) => (lbRecord.book_name,lbRecord.book_id)
////      case None=>("-99","-99")
////    }
////
////    val book_name_id = landPvUvRecord._1
////    println(s"book_name_id is:${book_name_id}")
////    val book_name = landPvUvRecord._2
////    println(s"book_name  is :${book_name}")
////
////  }
//}
