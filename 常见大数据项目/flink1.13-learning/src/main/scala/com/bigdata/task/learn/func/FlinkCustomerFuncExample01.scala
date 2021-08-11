package com.bigdata.task.learn.func

import org.apache.flink.table.api._
import org.apache.flink.table.functions.ScalarFunction
import scala.annotation.varargs
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.annotation.InputGroup
import org.apache.flink.types.Row
import java.math.BigDecimal

import org.apache.flink.table.annotation.FunctionHint
import org.apache.flink.table.functions.TableFunction

/**
 * @descr Flink Table Api/SQL 自定义函数
 * https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/functions/udfs/
 * @author lj.michale
 * @date 2021-08-11
 */
object FlinkCustomerFuncExample01 {

  def main(args: Array[String]): Unit = {

    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    val tableEnv = TableEnvironment.create(settings)

    tableEnv.executeSql(
      """
      CREATE TABLE GeneratedTable (
        a STRING,
        b INT,
        c INT,
        event_time TIMESTAMP_LTZ(3),
        rowtime AS PROCTIME(),
        WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
      )
      WITH ('connector'='datagen')
      """
    )

    val orders = tableEnv.from("GeneratedTable")
//    orders.select($"*").execute().print()

    /**
     * 展示了如何创建一个基本的标量函数，以及如何在 Table API 和 SQL 里调用这个函数。
     * 函数用于 SQL 查询前要先经过注册；而在用于 Table API 时，函数可以先注册后调用，也可以 内联 后直接使用。
     */
    // 在 Table API 里不经注册直接“内联”调用函数
    // tableEnv.from("GeneratedTable").select(call(classOf[SubstringFunction], $"a", 5, 12)).execute().print()

    // 注册函数
    tableEnv.createTemporarySystemFunction("SubstringFunction", classOf[SubstringFunction])
    // 在 Table API 里调用注册好的函数
    //tableEnv.from("GeneratedTable").select(call("SubstringFunction", $"a", 5, 12)).execute().print()

    // 在 SQL 里调用注册好的函数
    //tableEnv.sqlQuery("SELECT SubstringFunction(a, 5, 12) FROM GeneratedTable").execute().print()

    // 对于交互式会话，还可以在使用或注册函数之前对其进行参数化，这样可以把函数 实例 而不是函数 类 用作临时函数。
    // 为确保函数实例可应用于集群环境，参数必须是可序列化的。
    // 在 Table API 里不经注册直接“内联”调用函数
    // tableEnv.from("GeneratedTable").select(call(new SubstringFunction2(true), $"a", 5, 12)).execute().print()
    // 注册函数
    tableEnv.createTemporarySystemFunction("SubstringFunction2", new SubstringFunction2(true))
    // 在 SQL 里调用注册好的函数
    // tableEnv.sqlQuery("SELECT SubstringFunction2(a, 5, 12) FROM GeneratedTable").execute().print()

    ///////////////////// 运行时集成
    // 展示了如何在一个标量函数中通过 FunctionContext 来获取一个全局的任务参数：
    // 设置任务参数
    tableEnv.getConfig.addJobParameter("hashcode_factor", "31")
    // 注册函数
    tableEnv.createTemporarySystemFunction("hashCode", classOf[HashCodeFunction])
    // 调用函数
    tableEnv.sqlQuery("SELECT a, hashCode(a) FROM GeneratedTable").execute().print()

    ///////////////////// 标量函数


    ///////////////////// 表值函数
    // 在 Table API 里不经注册直接“内联”调用函数
    //    tableEnv
    //      .from("GeneratedTable")
    //      .joinLateral(call(classOf[SplitFunction], $"myField")
    // 注册函数
    tableEnv.createTemporarySystemFunction("SplitFunction", classOf[SplitFunction])
    // 在 Table API 里调用注册好的函数
//    tableEnv
//      .from("GeneratedTable")
//      .joinLateral(call("SplitFunction", $"a"))
//      .select($"a", $"b", $"c").execute().print()

    // 在 SQL 里调用注册好的函数
    // tableEnv.sqlQuery("SELECT a, b, c FROM GeneratedTable, LATERAL TABLE(SplitFunction(a))").execute().print()

    // 聚合函数
    tableEnv.createTemporaryFunction("wAvg", new WeightedAvg())
    // 使用函数
    // tableEnv.sqlQuery("SELECT a, wAvg(points, level) AS avgPoints FROM GeneratedTable GROUP BY a")

    tableEnv.createTemporaryFunction("top2", new Top2())
    // 使用函数
//    orders
//      .groupBy('key)
//      .flatAggregate(top2('a) as ('v, 'rank))
//      .select('key, 'v, 'rank)

  }

  // define function logic
  class SubstringFunction extends ScalarFunction {
    def eval(s: String, begin: Integer, end: Integer): String = {
      s.substring(begin, end)
    }
  }

  // 定义可参数化的函数逻辑
  class SubstringFunction2(endInclusive:Boolean) extends ScalarFunction {
    def eval(s: String, begin: Integer, end: Integer): String = {
      s.substring(begin, end)
    }
  }

  // 有多个重载求值方法的函数
  class SumFunction extends ScalarFunction {

    def eval(a: Integer, b: Integer): Integer = {
      a + b
    }

    def eval(a: String, b: String): Integer = {
      Integer.valueOf(a) + Integer.valueOf(b)
    }

    @varargs // generate var-args like Java
    def eval(d: Double*): Integer = {
      d.sum.toInt
    }
  }

  // function with overloaded evaluation methods
  class OverloadedFunction extends ScalarFunction {

    // no hint required
    def eval(a: Long, b: Long): Long = {
      a + b
    }

    // 定义 decimal 的精度和小数位
    @DataTypeHint("DECIMAL(12, 3)")
    def eval(a: Double, b: Double): BigDecimal = {
      BigDecimal.valueOf(a + b)
    }

    // 定义嵌套数据类型
    @DataTypeHint("ROW<s STRING, t TIMESTAMP_LTZ(3)>")
    def eval(i:Int): Row = {
      Row.of(java.lang.String.valueOf(i), java.time.Instant.ofEpochSecond(i))
    }

    // 允许任意类型的符入，并输出定制序列化后的值
//    @DataTypeHint(value = "RAW", bridgedTo = classOf[java.nio.ByteBuffer])
//    def eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o): java.nio.ByteBuffer = {
//      MyUtils.serializeToByteBuffer(o)
//    }
  }





}
