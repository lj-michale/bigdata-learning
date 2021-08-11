package com.bigdata.task.learn.tableapi

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.types.Row
import java.lang.{Integer => JInteger}

import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector

/**
 * @descr
 *
 * https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/tableapi/
 * @author lj.michale
 */
object TableAPIExample001 {

  def main(args: Array[String]): Unit = {

    // 环境配置
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
//    val result = orders.groupBy($"a")
//                .select($"a", $"b".count as "cnt")
//                .execute()
//                .print()

    val result22: Table = orders
      .filter($"a".isNotNull && $"b".isNotNull && $"c".isNotNull)
      .select($"a".lowerCase() as "a", $"b", $"event_time", $"rowtime")
      .window(Tumble over 1.hour on $"rowtime" as "hourlyWindow")
      .groupBy($"hourlyWindow", $"a")
      .select($"a", $"hourlyWindow".end as "hour", $"b".avg as "avgBillingAmount")

    tableEnv.createTemporaryView("t_result", result22)

//    tableEnv.executeSql("select * from t_result").print()

    val table22 = tableEnv.fromValues(
      row(1, "ABC"),
      row(2L, "ABCDE")
    )

    val table3 = tableEnv.fromValues(
      DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
        DataTypes.FIELD("name", DataTypes.STRING())
      ),
      row(1, "ABC"),
      row(2L, "ABCDE")
    )

    // 和 SQL 的 SELECT 子句类似。 执行一个 select 操作。
    val result4 = orders.select($"a", $"c" as "d")

    // （*）作为通配符，select 表中的所有列
    orders.select($"*")

    // 重命名字段
    tableEnv.from("GeneratedTable").as("x", "y", "z", "t")

    // 和 SQL 的 WHERE 子句类似。 过滤掉未验证通过过滤谓词的行。
    orders.filter($"a" % 2 === 0)

    //////////////////// 列操作
    // 执行字段添加操作。 如果所添加的字段已经存在，将抛出异常
    orders.addColumns(concat($"c", "Sunny"))

    // 执行字段添加操作。 如果添加的列名称和已存在的列名称相同，则已存在的字段将被替换。 此外，如果添加的字段里面有重复的字段名，则会使用最后一个字段。
    orders.addOrReplaceColumns(concat($"c", "Sunny") as "desc")

    // 删除列
    orders.dropColumns($"b", $"c")

    // 和 SQL 的 GROUP BY 子句类似。 使用分组键对行进行分组，使用伴随的聚合算子来按照组进行聚合行。
    orders.groupBy($"a").select($"a", $"b".sum().as("d"))

    // 使用分组窗口结合单个或者多个分组键对表进行分组和聚合。
    orders
      .window(Tumble over 5.minutes on $"rowtime" as "w") // 定义窗口
      .groupBy($"a", $"w") // 按窗口和键分组
      .select($"a", $"w".start, $"w".end, $"w".rowtime, $"b".sum as "d") // 访问窗口属性并聚合

    // 和 SQL 的 OVER 子句类似
    orders
      // 定义窗口
      .window(
        Over
          partitionBy $"a"
          orderBy $"rowtime"
          preceding UNBOUNDED_RANGE
          following CURRENT_RANGE
          as "w")
      .select($"a", $"b".avg over $"w", $"b".max().over($"w"), $"b".min().over($"w")) // 滑动聚合

    // 和 SQL DISTINCT 聚合子句类似，例如 COUNT(DISTINCT a)。 Distinct 聚合声明的聚合函数（内置或用户定义的）仅应用于互不相同的输入值。 Distinct 可以应用于 GroupBy Aggregation、GroupBy Window Aggregation 和 Over Window Aggregation。
    // 按属性分组后的的互异（互不相同、去重）聚合
    orders
      .groupBy($"a")
      .select($"a", $"b".sum.distinct as "d")
    // 按属性、时间窗口分组后的互异（互不相同、去重）聚合
    orders
      .window(Tumble over 5.minutes on $"rowtime" as "w").groupBy($"a", $"w")
      .select($"a", $"b".sum.distinct as "d")
    // over window 上的互异（互不相同、去重）聚合
    orders
      .window(Over
        partitionBy $"a"
        orderBy $"rowtime"
        preceding UNBOUNDED_RANGE
        as $"w")
      .select($"a", $"b".avg.distinct over $"w", $"b".max over $"w", $"b".min over $"w")

    ////////////////////////// Distinct
    orders.distinct()

    /////////////////////////// Joins
    //Inner Join
    // 和 SQL 的 JOIN 子句类似。关联两张表。两张表必须有不同的字段名，并且必须通过 join 算子或者使用 where 或 filter 算子定义至少一个 join 等式连接谓词。
    val left = tableEnv.from("MyTable").select($"a", $"b", $"c")
    val right = tableEnv.from("MyTable").select($"d", $"e", $"f")
    val result11 = left.join(right).where($"a" === $"d").select($"a", $"b", $"e")

    // Outer Join
    val leftOuterResult = left.leftOuterJoin(right, $"a" === $"d").select($"a", $"b", $"e")
    val rightOuterResult = left.rightOuterJoin(right, $"a" === $"d").select($"a", $"b", $"e")
    val fullOuterResult = left.fullOuterJoin(right, $"a" === $"d").select($"a", $"b", $"e")

    // Interval Join
    // Interval join 是可以通过流模式处理的常规 join 的子集。
    // Interval join 至少需要一个 equi-join 谓词和一个限制双方时间界限的 join 条件。
    // 这种条件可以由两个合适的范围谓词（<、<=、>=、>）或一个比较两个输入表相同时间属性（即处理时间或事件时间）的等值谓词来定义。
    val left1 = tableEnv.from("MyTable").select($"a", $"b", $"c", $"ltime")
    val right1 = tableEnv.from("MyTable").select($"d", $"e", $"f", $"rtime")
    val result23 = left1.join(right1)
      .where($"a" === $"d" && $"ltime" >= $"rtime" - 5.minutes && $"ltime" < $"rtime" + 10.minutes)
      .select($"a", $"b", $"e", $"ltime")

    // Inner Join with Table Function (UDTF)
    // join 表和表函数的结果。左（外部）表的每一行都会 join 表函数相应调用产生的所有行。 如果表函数调用返回空结果，则删除左侧（外部）表的一行。


    // Left Outer Join with Table Function (UDTF)
    // join 表和表函数的结果。左（外部）表的每一行都会 join 表函数相应调用产生的所有行。如果表函数调用返回空结果，则保留相应的 outer（外部连接）行并用空值填充右侧结果。
    // 目前，表函数左外连接的谓词只能为空或字面（常量）真。


    //////////////////////////// Set Operations

    //////////////////////////// OrderBy, Offset & Fetch
    // 和 SQL ORDER BY 子句类似。返回跨所有并行分区的全局有序记录。对于无界表，该操作需要对时间属性进行排序或进行后续的 fetch 操作。
    val result = orders.orderBy($"a".asc)

    // 和 SQL 的 OFFSET 和 FETCH 子句类似。Offset 操作根据偏移位置来限定（可能是已排序的）结果集。Fetch 操作将（可能已排序的）结果集限制为前 n 行。通常，这两个操作前面都有一个排序操作。对于无界表，offset 操作需要 fetch 操作。
    // 从已排序的结果集中返回前5条记录
    val result1: Table = orders.orderBy($"a".asc).fetch(5)
    // 从已排序的结果集中返回跳过3条记录之后的所有记录
    val result2: Table = orders.orderBy($"a".asc).offset(3)
    // 从已排序的结果集中返回跳过10条记录之后的前5条记录
    val result3: Table = orders.orderBy($"a".asc).offset(10).fetch(5)

    ///////////////////////////// Insert
    // 和 SQL 查询中的 INSERT INTO 子句类似，该方法执行对已注册的输出表的插入操作。executeInsert() 方法将立即提交执行插入操作的 Flink job。
    // 输出表必须已注册在 TableEnvironment（详见表连接器）中。此外，已注册表的 schema 必须与查询中的 schema 相匹配。
    orders.executeInsert("OutOrders")

    ///////////////////////////// Group Windows
    // Group window 聚合根据时间或行计数间隔将行分为有限组，并为每个分组进行一次聚合函数计算。对于批处理表，窗口是按时间间隔对记录进行分组的便捷方式。
    // Tumble (Tumbling Windows)
    // 滚动窗口将行分配给固定长度的非重叠连续窗口。例如，一个 5 分钟的滚动窗口以 5 分钟的间隔对行进行分组。滚动窗口可以定义在事件时间、处理时间或行数上。

    // Slide (Sliding Windows)
    // 滑动窗口具有固定大小并按指定的滑动间隔滑动。如果滑动间隔小于窗口大小，则滑动窗口重叠。因此，行可能分配给多个窗口。
    // 例如，15 分钟大小和 5 分钟滑动间隔的滑动窗口将每一行分配给 3 个不同的 15 分钟大小的窗口，以 5 分钟的间隔进行一次计算。滑动窗口可以定义在事件时间、处理时间或行数上。

    // Session (Session Windows)
    // 会话窗口没有固定的大小，其边界是由不活动的间隔定义的，例如，如果在定义的间隔期内没有事件出现，则会话窗口将关闭。
    // 例如，定义30 分钟间隔的会话窗口，当观察到一行在 30 分钟内不活动（否则该行将被添加到现有窗口中）且30 分钟内没有添加新行，窗口会关闭。会话窗口支持事件时间和处理时间。

    ///////////////////////////// Over Windows
    // Over window 聚合聚合来自在标准的 SQL（OVER 子句），可以在 SELECT 查询子句中定义。
    // 与在“GROUP BY”子句中指定的 group window 不同， over window 不会折叠行。相反，over window 聚合为每个输入行在其相邻行的范围内计算聚合。
    // Over windows 使用 window(w: OverWindow*) 子句（在 Python API 中使用 over_window(*OverWindow)）定义，并通过 select() 方法中的别名引用。
    // 以下示例显示如何在表上定义 over window 聚合。
//    val table = input
//      .window([w: OverWindow] as $"w")              // define over window with alias w
//    .select($"a", $"b".sum over $"w", $"c".min over $"w") // aggregate over the over window w


    ///////////////////// Row-based Operations
    val func = new MyMapFunction()
    val table111 = orders.map(func($"c")).as("a", "b")

    val func2 = new MyFlatMapFunction
    val table2 = orders.flatMap(func($"c")).as("a", "b")

    val myAggFunc = new MyMinMax
    val table1111 = orders
      .groupBy($"key")
      .aggregate(myAggFunc($"a") as ("x", "y"))
      .select($"key", $"x", $"y")

    /////////////////////// Group Window Aggregate
    // 在 group window 和可能的一个或多个分组键上对表进行分组和聚合。你必须使用 select 子句关闭 aggregate。并且 select 子句不支持“*“或聚合函数。

    // FlatAggregate
    // 和 GroupBy Aggregation 类似。使用运行中的表之后的聚合运算符对分组键上的行进行分组，以按组聚合行。和 AggregateFunction 的不同之处在于，TableAggregateFunction 的每个分组可能返回0或多条记录。你必须使用 select 子句关闭 flatAggregate。并且 select 子句不支持聚合函数。
    //除了使用 emitValue 输出结果，你还可以使用 emitUpdateWithRetract 方法。和 emitValue 不同的是，emitUpdateWithRetract 用于发出已更新的值。此方法在retract 模式下增量输出数据，例如，一旦有更新，我们必须在发送新的更新记录之前收回旧记录。如果在表聚合函数中定义了这两个方法，则将优先使用 emitUpdateWithRetract 方法而不是 emitValue 方法，这是因为该方法可以增量输出值，因此被视为比 emitValue 方法更有效。
    val top2 = new Top2
    val result555 = orders
      .groupBy($"key")
      .flatAggregate(top2($"a"))
      .select($"key", $"v", $"rank")


  }

  // 使用用户定义的标量函数或内置标量函数执行 map 操作。如果输出类型是复合类型，则输出将被展平。
  class MyMapFunction extends ScalarFunction {
    def eval(a: String): Row = {
      Row.of(a, "pre-" + a)
    }

    override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
      Types.ROW(Types.STRING, Types.STRING)
  }

  // 使用表函数执行 flatMap 操作。
  class MyFlatMapFunction extends TableFunction[Row] {
    def eval(str: String): Unit = {
      if (str.contains("#")) {
        str.split("#").foreach({ s =>
          val row = new Row(2)
          row.setField(0, s)
          row.setField(1, s.length)
          collect(row)
        })
      }
    }

    override def getResultType: TypeInformation[Row] = {
      Types.ROW(Types.STRING, Types.INT)
    }
  }

  // 使用聚合函数来执行聚合操作。你必须使用 select 子句关闭 aggregate，并且 select 子句不支持聚合函数。如果输出类型是复合类型，则聚合的输出将被展平。
  class MyMinMax extends AggregateFunction[Row, MyMinMaxAcc] {

    def accumulate(acc: MyMinMaxAcc, value: Int): Unit = {
      if (value < acc.min) {
        acc.min = value
      }
      if (value > acc.max) {
        acc.max = value
      }
    }

    override def createAccumulator(): MyMinMaxAcc = MyMinMaxAcc(0, 0)

    def resetAccumulator(acc: MyMinMaxAcc): Unit = {
      acc.min = 0
      acc.max = 0
    }

    override def getValue(acc: MyMinMaxAcc): Row = {
      Row.of(Integer.valueOf(acc.min), Integer.valueOf(acc.max))
    }

    override def getResultType: TypeInformation[Row] = {
      new RowTypeInfo(Types.INT, Types.INT)
    }
  }







  case class MyMinMaxAcc(var min: Int, var max: Int)

}
