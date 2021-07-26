package com.bigdata.task.learn.flinksql

import java.util.{Random, UUID}

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, GroupWindow, Table, Tumble}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
//import org.apache.flink.table.api.Expressions.$

/**
 * @author lj.michale
 * @description FlinkSQLExample001
 * @date 2021-05-20
 */
object FlinkSQLExample0003 {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val paramTool: ParameterTool = ParameterTool.fromArgs(args)

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint")
    env.setStateBackend(new EmbeddedRocksDBStateBackend)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val ds: DataStream[(String, String, Double, Long)] = env.addSource(new MyExampleSource)

    //DataStream 转成 Table
//    val orders: Table = tableEnv.fromDataStream(ds, $("user"), $("product"), $("amount"), $("timestamp"))
//    orders.execute().print()
//    +----+--------------------------------+--------------------------------+--------------------------------+----------------------+
//    | op |                           user |                        product |                         amount |            timestamp |
//    +----+--------------------------------+--------------------------------+--------------------------------+----------------------+
//    | +I | 6fff8c2cf9d94313b7ed91f0262... |                       欢乐薯条 |                         180.34 |        1627202028550 |
//      | +I | eeae1126f47347df8aab195134c... |                          J帽子 |                         976.33 |        1627202028550 |
//      | +I | 49505a2edacd493ca87c3793852... |                           衣服 |                         180.34 |        1627202028550 |
//      | +I | 55e61b40d84c4a91b3f9dba3db5... |                           蜂蜜 |                           5.45 |        1627202028550 |
//      | +I | 8a50adc48ce2483eb45b1b8bf48... |                           鞋子 |                           5.45 |        1627202028550 |
//      | +I | 2da35df918664a019756abc573c... |                           衣服 |                           5.45 |        1627202028550 |
//      | +I | a0ead7da18a24a7384b34755baf... |                           蜂蜜 |                          82.12 |        1627202028550 |
//      | +I | b41b6c842f9640c6ac04855fdd9... |                       RubberOK |                         976.33 |        1627202028550 |
//      | +I | 1f1301d74bdf4b80a70c83cca68... |                           蜂蜜 |                          46.78 |        1627202028550 |

    // 查询2: 先将DataStream转成临时表，然后查询
//    tableEnv.createTemporaryView("t_order", ds, $("user"), $("product"), $("amount"), $("buy_time"))
//    val query2: Table = tableEnv.sqlQuery("SELECT user,product,amount,buy_time  FROM t_order WHERE product LIKE '%Rubber%'")
//    query2.execute().print()
//    +----+--------------------------------+--------------------------------+--------------------------------+----------------------+
//    | op |                           user |                        product |                         amount |            timestamp |
//    +----+--------------------------------+--------------------------------+--------------------------------+----------------------+
//    | +I | 3edb949c64794a2f94843558d39... |                       RubberOK |                         107.25 |        1627202428622 |
//    | +I | 83579d7390b14fa18306054414d... |                       RubberOK |                          18.23 |        1627202428622 |
//    | +I | 35dc0d4733304d188b2fa93ef65... |                       RubberOK |                         107.25 |        1627202428622 |

    // 查询3:
//    val querySQL3:String =
//      """
//        |SELECT user,product,amount,buy_time
//        |FROM t_order
//        |WHERE product LIKE '%Rubber%'
//        |""".stripMargin
//    val query3: Table = tableEnv.sqlQuery(querySQL3)
//    query3.execute().print()
//    +----+--------------------------------+--------------------------------+--------------------------------+----------------------+
//    | op |                           user |                        product |                         amount |             buy_time |
//    +----+--------------------------------+--------------------------------+--------------------------------+----------------------+
//    | +I | 0099b82d2d4c46069510b9ed417... |                       RubberOK |                          82.12 |        1627202772831 |
//    | +I | d3feee1bf46f47a1a1792c38947... |                       RubberOK |                         976.33 |        1627202772831 |
//    | +I | 76545f9ed63e42a39496a0e1898... |                       RubberOK |                          98.12 |        1627202772831 |
//    16:47:26,183 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator     - Triggering checkpoint 2 (type=CHECKPOINT) @ 1627202846181 for job 9339cc6c9d53dfb3f328edbdc5f89046.
//    16:47:26,216 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator     - Completed checkpoint 2 for job 9339cc6c9d53dfb3f328edbdc5f89046 (674 bytes in 21 ms).
//    | +I | 64040d0f533d4f3187810256299... |                       RubberOK |                          46.78 |        1627202772831 |
//    | +I | 84a3e5cdb1fa492aadf4d41c540... |                       RubberOK |                           5.45 |        1627202772831 |

//    // 查询4: 加上SUM之后有报错，暂时未能解决
//    val querySQL4:String =
//      """
//        |SELECT user,product,SUM(amount),buy_time
//        |FROM t_order
//        |WHERE product LIKE '%Rubber%'
//        |GROUP BY user,product, buy_time
//        |""".stripMargin
//    val query4: Table = tableEnv.sqlQuery(querySQL4)
//    query4.execute().print()

    // 查询5
//    tableEnv.sqlQuery("SELECT user,product,amount,buy_time  FROM t_order")
//      .filter($("user").isNotNull)
//      .filter($("product").isNotNull)
//      .filter($("amount").isNotNull).execute().print()

    // 查询6
//    tableEnv.sqlQuery("SELECT user,product,amount,buy_time FROM t_order")
//      .filter($("user").isNotNull)
//      .select($("user").lowerCase() as "user", $("product"), $("amount"), $("buy_time"))
//      .execute().print()

    // 查询7
//    tableEnv.sqlQuery("SELECT user,product,amount,buy_time FROM t_order")
//      .filter($("user").isNotNull)

    /**
     * Query a Table By Table Api Flink1.13
     *
     * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/common/
     *
     * notice:
     * org.apache.flink.table.api._ -     for implicit expression conversions
     * org.apache.flink.api.scala._ and org.apache.flink.table.api.bridge.scala._    if you want to convert from/to DataStream.
     */
    import org.apache.flink.table.api.bridge.scala._
    import org.apache.flink.table.api._
    import org.apache.flink.api.scala._

    // 查询-0001.加上sum有报错
//    val revenue = tableEnv.sqlQuery("SELECT user,product,amount,buy_time FROM t_order")
////      .filter($"user" === "RubberOK")
//      .groupBy($"user", $"product")
//      .select($"user", $"product", $"amount".sum)
//      .execute().print()

    // https://blog.csdn.net/appleyuchi/article/details/109163654
    tableEnv.createTemporaryView("t_order", ds, $("user"), $("product"), $("amount"), $("buy_time"))
    tableEnv.sqlQuery("SELECT user,product,amount,buy_time FROM t_order")
      .filter(and($("user").isNotNull(), $("product").isNotNull(), $("amount").isNotNull()))
      .select($("user"), $("amount"), $("buy_time"))
      .window(Tumble.over(lit(1).minutes()).on($("rowtime")).as("hourlyWindow"))
      .groupBy($("hourlyWindow"), $("user"))
      .select($("user"), $("hourlyWindow").end().as("hour"), $("amount").avg().as("avgBillingAmount"))
      .execute().print()

    /**
     * Query a Table By Table SQL Flink1.13
     *
     * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/common/
     */
//    tableEnv.sqlQuery("SELECT user,product,amount,buy_time FROM t_order").execute().print()

//    val counts:Table = orders
//      .groupBy($("user"))
//      .select($("user"), $("product").count().as("cnt"))
//    counts.execute().print()

    //val result: Table = tableEnv.sqlQuery("SELECT SUM(amount) FROM " + orders + " WHERE product LIKE '%Rubber%'")


    env.execute("FlinkSQLExample0003")

  }


  /**
   * @descr 自定义source数据集
   */
  class MyExampleSource() extends SourceFunction[(String, String, Double, Long)] {

    var running = true
    var timestamp = System.currentTimeMillis()

    /**
     * @descr run
     * @param sourceContext
     */
    override def run(sourceContext: SourceContext[(String, String, Double, Long)]): Unit = {
      while (running) {
        val random: Random = new Random()
        val userId:String = getRandomUserId()
        val productName:String = getRandomProductName(random)
        val amount:Double = getRandomAmount(random)
        Thread.sleep(1000)
        sourceContext.collect((userId, productName, amount, timestamp))
      }
    }

    override def cancel(): Unit = {
      running = false
    }

    /**
     * @descr 产生随机UserId
     */
    def getRandomUserId(): String = {
      UUID.randomUUID.toString.replaceAll("-", "")
    }

    /**
     * @descr 产生随机ProductName
     */
    def getRandomProductName(random: Random): String = {
      val array = Array("欢乐薯条", "衣服", "鞋子", "J帽子", "《安徒生童话全集》", "蜂蜜", "盆景", "《SpringCloud微服务架构设计与开发》","RubberOK")
      array(random.nextInt(array.length))
    }

    /**
     * @descr 产生随机amount
     */
    def getRandomAmount(random: Random): Double = {
      val array = Array(18.23, 5.45, 46.78, 98.12, 976.33, 234.76, 82.12, 107.25, 180.34)
      array(random.nextInt(array.length))
    }

  }

  case class order(userId:String, productName:String, amount:Double, buyTime:Long)

}
