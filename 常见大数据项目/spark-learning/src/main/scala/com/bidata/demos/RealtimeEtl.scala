package com.bidata.demos


import com.alibaba.fastjson.{JSON, JSONException}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
 * created by fwmagic
 */
object RealtimeEtl {

  private val logger = LoggerFactory.getLogger(RealtimeEtl.getClass)

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val conf = new SparkConf().setAppName("RealtimeEtl").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val streamContext = new StreamingContext(spark.sparkContext, Seconds(5))

    //直连方式相当于跟kafka的Topic至直接连接
    //"auto.offset.reset:earliest(每次重启重新开始消费)，latest(重启时会从最新的offset开始读取)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hd1:9092,hd2:9092,hd3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "fwmagic",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("access")

    val kafkaDStream = KafkaUtils.createDirectStream[String, String](
      streamContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //如果使用SparkStream和Kafka直连方式整合，生成的kafkaDStream必须调用foreachRDD
    kafkaDStream.foreachRDD(kafkaRDD => {
      if (!kafkaRDD.isEmpty()) {
        //获取当前批次的RDD的偏移量
        val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

        //拿出kafka中的数据
        val lines = kafkaRDD.map(_.value())
        //将lines字符串转换成json对象
        val logBeanRDD = lines.map(line => {
          var logBean: LogBean = null
          try {
            logBean = JSON.parseObject(line, classOf[LogBean])
          } catch {
            case e: JSONException => {
              //logger记录
              logger.error("json解析错误！line:" + line, e)
            }
          }
          logBean
        })

        //过滤
        val filteredRDD = logBeanRDD.filter(_ != null)

        //将RDD转化成DataFrame,因为RDD中装的是case class
        import spark.implicits._

        val df = filteredRDD.toDF()

        df.show()
        //将数据写到hdfs中:hdfs://hd1:9000/360
        df.repartition(1).write.mode(SaveMode.Append).parquet(args(0))

        //提交当前批次的偏移量，偏移量最后写入kafka
        kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })

    //启动
    streamContext.start()
    streamContext.awaitTermination()
    streamContext.stop()

  }

}

case class LogBean(time:String,
                   longitude:Double,
                   latitude:Double,
                   openid:String,
                   page:String,
                   evnet_type:Int)