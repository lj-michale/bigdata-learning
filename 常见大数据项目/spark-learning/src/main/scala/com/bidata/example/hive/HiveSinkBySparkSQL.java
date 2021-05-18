package com.bidata.example.hive;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @author lj.michale
 * @description 读写数据的时候使用分区 partitionBy()
 * @date 2021-05-18
 */

public class HiveSinkBySparkSQL {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .config("spark.driver.host", "localhost")
                .appName("FilePartitionTest")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> dataset_1 =
                spark.read().parquet("/trackerSession_date");
        dataset_1.show();
//      +--------------------+-------------------+------------+-------+----+------+--------+
//      |          session_id|session_server_time|cookie_label| cookie|year| month|     day|
//      +--------------------+-------------------+------------+-------+----+------+--------+
//      |520815c9-bdd4-40c...|2017-09-03 12:00:00|          固执|cookie1|2017|201709|20170903|
//      |912a4b47-6984-476...|2017-09-03 12:00:00|          固执|cookie1|2017|201709|20170903|
//      |520815c9-bdd4-40c...|2017-09-04 12:00:00|          固执|cookie1|2017|201709|20170904|
//      |912a4b47-6984-476...|2017-09-04 12:45:01|          固执|cookie1|2017|201709|20170904|
//      |79534f7c-b4dc-4bc...|2017-09-04 12:00:01|         有偏见|cookie2|2017|201709|20170904|
//      |79534f7c-b4dc-4bc...|2017-09-03 12:00:00|         有偏见|cookie2|2017|201709|20170903|
//      +--------------------+-------------------+------------+-------+----+------+--------+

        dataset_1.createOrReplaceTempView("non_partition_table");
        spark.sql("select * from non_partition_table where day = 20170903").show();
//      +--------------------+-------------------+------------+-------+----+------+--------+
//      |          session_id|session_server_time|cookie_label| cookie|year| month|     day|
//      +--------------------+-------------------+------------+-------+----+------+--------+
//      |520815c9-bdd4-40c...|2017-09-03 12:00:00|          固执|cookie1|2017|201709|20170903|
//      |912a4b47-6984-476...|2017-09-03 12:00:00|          固执|cookie1|2017|201709|20170903|
//      |79534f7c-b4dc-4bc...|2017-09-03 12:00:00|         有偏见|cookie2|2017|201709|20170903|
//      +--------------------+-------------------+------------+-------+----+------+--------+

        //保存的时候，对数据按照年月日进行分区 ，关键 partitionBy("year", "month", "day")
        dataset_1.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("year", "month", "day")
                .parquet( "/trackerSession_partition");

        Dataset<Row> partitionDF =
                spark.read().parquet("/trackerSession_partition");
        partitionDF.show();
//注意看结果是有一定的顺序的
//      +--------------------+-------------------+------------+-------+----+------+--------+
//      |          session_id|session_server_time|cookie_label| cookie|year| month|     day|
//      +--------------------+-------------------+------------+-------+----+------+--------+
//      |520815c9-bdd4-40c...|2017-09-03 12:00:00|          固执|cookie1|2017|201709|20170903|
//      |912a4b47-6984-476...|2017-09-03 12:00:00|          固执|cookie1|2017|201709|20170903|
//      |79534f7c-b4dc-4bc...|2017-09-03 12:00:00|         有偏见|cookie2|2017|201709|20170903|
//      |520815c9-bdd4-40c...|2017-09-04 12:00:00|          固执|cookie1|2017|201709|20170904|
//      |912a4b47-6984-476...|2017-09-04 12:45:01|          固执|cookie1|2017|201709|20170904|
//      |79534f7c-b4dc-4bc...|2017-09-04 12:00:01|         有偏见|cookie2|2017|201709|20170904|
//      +--------------------+-------------------+------------+-------+----+------+--------+

        //用sql查询某20170903这天的数据
        partitionDF.createOrReplaceTempView("partition_table");
        spark.sql("select * from partition_table where day = 20170903").show();
//      +--------------------+-------------------+------------+-------+----+------+--------+
//      |          session_id|session_server_time|cookie_label| cookie|year| month|     day|
//      +--------------------+-------------------+------------+-------+----+------+--------+
//      |520815c9-bdd4-40c...|2017-09-03 12:00:00|          固执|cookie1|2017|201709|20170903|
//      |912a4b47-6984-476...|2017-09-03 12:00:00|          固执|cookie1|2017|201709|20170903|
//      |79534f7c-b4dc-4bc...|2017-09-03 12:00:00|         有偏见|cookie2|2017|201709|20170903|
//      +--------------------+-------------------+------------+-------+----+------+--------+

        //直接读目录，取20170903这天的数据
        Dataset<Row> day03DF =
                spark.read()
                        .parquet("/trackerSession_partition/year=2017/month=201709/day=20170903");
        day03DF.show();
//注意看这里的结果，没有了分区的字段信息
//      +--------------------+-------------------+------------+-------+
//      |          session_id|session_server_time|cookie_label| cookie|
//      +--------------------+-------------------+------------+-------+
//      |520815c9-bdd4-40c...|2017-09-03 12:00:00|          固执|cookie1|
//      |912a4b47-6984-476...|2017-09-03 12:00:00|          固执|cookie1|
//      |79534f7c-b4dc-4bc...|2017-09-03 12:00:00|         有偏见|cookie2|
//      +--------------------+-------------------+------------+-------+

        //bucket只能用于hive表中
        dataset_1.write()
                .partitionBy("year")
                .bucketBy(24, "cookie")
                .saveAsTable("partition_session");

        spark.stop();
    }
}
