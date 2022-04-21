package com.bigdata.stream.example001

import com.bigdata.core.BaseApp
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.JValue
import org.json4s.jackson.{JsonMethods, Serialization}
import com.bigdata.kafka.{KafkaOffsetManagerUtils, SparkKafkaSinkUtils, SparkKafkaUtils}

import scala.collection.mutable.ListBuffer

object BaseDBCanalApp extends BaseApp{

  override val master: String = "local[2]"
  override val appName: String = "BaseDBCannalApp"
  override val groupId: String = "BaseDBCanalApp"
  override val topic: String = "gmall_db"
  override val bachTime: Int = 3

  val tableNames = List(
    "order_info",  //
    "order_detail",
    "user_info",
    "base_province",
    "base_category3",
    "sku_info",
    "spu_info",
    "base_trademark")

  override def run(ssc: StreamingContext,
                   offsetRanges: ListBuffer[OffsetRange],
                   sourceStream: DStream[String]): Unit = {



    //分流
//    sourceStream.flatMap(str =>{
//
//      implicit val f = org.json4s.DefaultFormats
//      val  j: JValue = JsonMethods.parse(str)
//      val data: JValue = j \ "data" //一个集合或者数组
//      val tableName: String = (j \ "table").extract[String]
//      val operate: String = (j \ "type").extract[String]
//
//      //拿到date集合的每个对象
//      //date.children.map(child =>(tableName,operate,JsonMethods.compact(JsonMethods.render(child))))
//      date.children.map(child =>(tableName,operate.toLowerCase(),Serialization.write(child)))
//    })
//      //过滤出数据
//      .filter{
//        case (tableName,operate,data)=>
//          //主要满足要求的表，和非删除的数据和内容不能是0
//          tableNames.contains(tableName) && operate != "delete"  &&data.length>0
//      }
//      .foreachRDD(rdd =>{
//        //写入到ODS层（kafka）
//        rdd.foreachPartition((it:Iterator[(String,String,String)]) =>{
//          //先获取一个kafka的生产者
//          val producer: KafkaProducer[String, String] = SparkKafkaSinkUtils.createKafkaProducer
//          //写入
//          it.foreach{
//            case (tableName,operate,data)=>{
//              val topic = s"ods_$tableName"
//              if(tableName !="order_info"){
//                producer.send(new ProducerRecord[String, String](topic, data))
//              }else if (operate == "insert"){
//                producer.send(new ProducerRecord[String, String](topic, data))
//              }
//            }
//          }
//          //关闭生产者
//          producer.close()
//        })
//        KafkaOffsetManagerUtils.saveOffset(groupId, topic, offsetRanges)
//      })

  }

}
