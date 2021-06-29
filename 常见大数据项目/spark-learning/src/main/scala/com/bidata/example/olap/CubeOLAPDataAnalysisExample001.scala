package com.bidata.example.olap

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author lj.michale
 * @description Spark多维分析cube/rollup/grouping sets/group by
 * @date 2021-06-25
 */
object CubeOLAPDataAnalysisExample001 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //测试数据
    val honorSeq = Seq(
        MemberInfo("战争学院区", "王者", "王者1连胜", 25),
        MemberInfo("战争学院区", "王者", "王者1连胜", 25),
        MemberInfo("战争学院区", "王者", "王者3连胜", 70),
        MemberInfo("战争学院区", "王者", "王者12连胜", 300),
        MemberInfo("战争学院区", "大师", "大师3连胜", 60),
        MemberInfo("战争学院区", "大师", "大师3连胜", 60),
        MemberInfo("战争学院区", "大师", "大师6连胜", 120),
        MemberInfo("战争学院区", "黄金", "黄金1连胜", 15),
        MemberInfo("战争学院区", "黄金", "黄金1连胜", 15),
        MemberInfo("战争学院区", "黄金", "黄金3连胜", 45),
        MemberInfo("战争学院区", "黄金", "黄金12连胜", 180),
        MemberInfo("裁决之地区", "王者", "王者1连胜", 25),
        MemberInfo("裁决之地区", "王者", "王者1连胜", 25),
        MemberInfo("裁决之地区", "大师", "大师3连胜", 60),
        MemberInfo("裁决之地区", "黄金", "黄金3连胜", 45),
        MemberInfo("诺克萨斯区", "王者", "王者1连胜", 25),
        MemberInfo("诺克萨斯区", "王者", "王者1连胜", 25),
        MemberInfo("诺克萨斯区", "大师", "大师3连胜", 60),
        MemberInfo("诺克萨斯区", "黄金", "黄金3连胜", 45)
    )

    import spark.implicits._
    val honorDF: DataFrame = honorSeq.toDF()
    honorDF.createOrReplaceTempView("temp")

    // 维度分析 GROUP BY
    // group by是SELECT语句的从句，用来指定查询分组条件，主要用来对查询的结果进行分组，相同组合的分组条件在结果集中只显示一行记录。
    // 使用group by从句时候，通过添加聚合函数（主要有COUNT()、SUM、MAX()、MIN()等）可以使数据聚合.

    //group by sum  sql风格
    val groupByHonorDF: DataFrame = spark.sql("select area,grade,honor,sum(value) as total_value from temp group by area,grade,honor")
    groupByHonorDF.show()

    //DSL风格
    import org.apache.spark.sql.functions._
    val groupByHonorDFDSL: DataFrame = honorDF.groupBy("area", "grade", "honor").agg(sum("value").alias("total_value"))
    groupByHonorDFDSL.show()

    // grouping sets
    // a.grouping sets是group by子句更进一步的扩展, 它让你能够定义多个数据分组。这样做使聚合更容易, 并且因此使得多维数据分析更容易。
    // b.够用grouping sets在同一查询中定义多个分组;
    // eg：group by A,B grouping sets(A,B)就等价于 group by A union group by B；
    // group by A,B,C grouping sets((A,C),(A,B))就等价于 group by A,C union group by A,B;
    val groupingHonorDF: DataFrame = spark.sql("select area,grade,honor,sum(value) as total_value from temp group by area,grade,honor grouping sets(area,grade,honor)")
    groupingHonorDF.show()

    // rollup
    // rollup 是根据维度在数据结果集中进行的聚合操作。
    // group by A,B,C with rollup首先会对(A、B、C)进行group by，然后对(A、B)进行group by，然后是(A)进行group by，最后对各个分组结果进行union操作。
    //sql风格
    val rollupHonorDF: DataFrame = spark.sql("select area,grade,honor,sum(value) as total_value from temp group by area,grade,honor with rollup")
    val rollupHonorDFDSL: DataFrame = honorDF.rollup("area", "grade", "honor").agg(sum("value").alias("total_value"))
    rollupHonorDF.show()
    rollupHonorDFDSL.show()

    // cube
    // group by A,B,C with cube，则首先会对(A、B、C)进行group by，然后依次是(A、B)，(A、C)，(A)，(B、C)，(B)，( C)，最后对全表进行group by操作。
    val rollupHonorDFDSL2: DataFrame = honorDF.cube("area", "grade", "honor").agg(sum("value").alias("total_value"))
    val cubeHonorDF: DataFrame = spark.sql("select area,grade,honor,sum(value) as total_value from temp group by area,grade,honor with cube")
    rollupHonorDFDSL2.show()
    cubeHonorDF.show()


    spark.stop()
    sc.stop()

  }

  case class MemberInfo(area: String, grade: String, honor: String, value: Int)

}
