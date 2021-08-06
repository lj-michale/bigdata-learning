package com.bigdata.connector.mysql

import java.sql.{DriverManager, ResultSet}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory


class JdbcSourceV1 extends RelationProvider {

  override def createRelation( sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    new JdbcRelationV1(
      parameters("url"),
      parameters("user"),
      parameters("password"),
      parameters("table")
    )(sqlContext.sparkSession)
  }

}

class JdbcRelationV1( url: String,
                      user: String,
                      password: String,
                      table: String)(@transient val sparkSession: SparkSession)
  extends BaseRelation with PrunedFilteredScan {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = StructType(Seq(
    StructField("id", IntegerType),
    StructField("emp_name", StringType),
    StructField("dep_name", StringType),
    StructField("salary", DecimalType(7, 2)),
    StructField("age", DecimalType(3, 0))
  ))

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    new JdbcRDD(sparkSession.sparkContext, requiredColumns, filters, url, user, password, table, schema)
  }

}


class JdbcRDD( sc: SparkContext,
               columns: Array[String],
               filters: Array[Filter],
               url: String,
               user: String,
               password: String,
               table: String,
               schema: StructType)
  extends RDD[Row](sc, Nil) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {

    val sqlBuilder = new StringBuilder()
    sqlBuilder.append(s"SELECT ${columns.mkString(", ")} FROM $table")

    val wheres = filters.flatMap {
      case EqualTo(attribute, value) => Some(s"$attribute = '$value'")
      case _ => None
    }
    if (wheres.nonEmpty) {
      sqlBuilder.append(s" WHERE ${wheres.mkString(" AND ")}")
    }

    val sql = sqlBuilder.toString
    logger.info(sql)

    val conn = DriverManager.getConnection(url, user, password)
    val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stmt.setFetchSize(1000)
    val rs = stmt.executeQuery()

    context.addTaskCompletionListener(_ => conn.close())

    new Iterator[Row] {
      def hasNext: Boolean = rs.next()
      def next: Row = {
        val values = columns.map {
          case "id" => rs.getInt("id")
          case "emp_name" => rs.getString("emp_name")
          case "dep_name" => rs.getString("dep_name")
          case "salary" => rs.getBigDecimal("salary")
          case "age" => rs.getBigDecimal("age")
        }
        Row.fromSeq(values)
      }
    }
  }

  override protected def getPartitions: Array[Partition] = Array(JdbcPartition(0))

}

case class JdbcPartition(idx: Int) extends Partition {
  override def index: Int = idx
}


object JdbcExampleV1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    val df = spark.read
      .format("com.bigdata.connector.mysql.JdbcSourceV1")
      .option("url", "jdbc:mysql://localhost/bigdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true")
      .option("user", "root")
      .option("password", "abc1314520")
      .option("table", "employee")
      .load()

    df.printSchema()
    df.show()

    spark.sql(
      """
        |CREATE TEMPORARY VIEW employee
        |USING com.bigdata.connector.mysql.JdbcSourceV1
        |OPTIONS (
        |  url 'jdbc:mysql://localhost/bigdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true',
        |  user 'root',
        |  password 'abc1314520',
        |  table 'employee'
        |)
      """.stripMargin)
    val dfSelect = spark.sql("SELECT COUNT(*), AVG(salary) FROM employee WHERE dep_name = 'Management'")
    dfSelect.explain(true)
    /*
        == Parsed Logical Plan ==
        'Project [unresolvedalias('COUNT(1), None), unresolvedalias('AVG('salary), None)]
        +- 'Filter ('dep_name = Management)
        +- 'UnresolvedRelation `employee`

        == Analyzed Logical Plan ==
        count(1): bigint, avg(salary): decimal(11,6)
        Aggregate [count(1) AS count(1)#45L, avg(salary#29) AS avg(salary)#46]
        +- Filter (dep_name#28 = Management)
        +- SubqueryAlias `employee`
        +- Relation[id#26,emp_name#27,dep_name#28,salary#29,age#30] com.bigdata.connector.mysql.JdbcRelationV1@2c2edbe7

        == Optimized Logical Plan ==
        Aggregate [count(1) AS count(1)#45L, cast((avg(UnscaledValue(salary#29)) / 100.0) as decimal(11,6)) AS avg(salary)#46]
        +- Project [salary#29]
        +- Filter (isnotnull(dep_name#28) && (dep_name#28 = Management))
        +- Relation[id#26,emp_name#27,dep_name#28,salary#29,age#30] com.bigdata.connector.mysql.JdbcRelationV1@2c2edbe7

        == Physical Plan ==
          *(2) HashAggregate(keys=[], functions=[count(1), avg(UnscaledValue(salary#29))], output=[count(1)#45L, avg(salary)#46])
        +- Exchange SinglePartition
        +- *(1) HashAggregate(keys=[], functions=[partial_count(1), partial_avg(UnscaledValue(salary#29))], output=[count#50L, sum#51, count#52L])
        +- *(1) Project [salary#29]
        +- *(1) Filter (isnotnull(dep_name#28) && (dep_name#28 = Management))
        +- *(1) Scan com.bigdata.connector.mysql.JdbcRelationV1@2c2edbe7 [salary#29,dep_name#28] PushedFilters: [IsNotNull(dep_name), EqualTo(dep_name,Management)], ReadSchema: struct<salary:decimal(7,2),dep_name:string>
    */

    dfSelect.show()

    spark.stop()
  }
}