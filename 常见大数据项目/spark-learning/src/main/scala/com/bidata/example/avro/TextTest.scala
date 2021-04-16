package com.bidata.example.avro

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.NullWritable
import org.apache.avro.Schema
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.avro.mapreduce._

object TextTest extends App {

  System.setProperty("hadoop.home.dir", "D:\\bd\\software\\winutils")
  val sparkConf = new SparkConf().setAppName("AvroTest").setMaster("local[4]")
  val sc = new SparkContext(sparkConf)

  //**************************to generate an avro file based on internal java type
  var li = List("A","A","C","B")
  var lip = sc.parallelize(li, 4)
  var liprdd = lip.map { x => (new AvroKey[String](x),NullWritable.get()) }
  var prdd = new PairRDDFunctions(liprdd)
  val schema = Schema.create(Schema.Type.STRING)
  val job1 = Job.getInstance

  AvroJob.setOutputKeySchema(job1, schema)

  prdd.saveAsNewAPIHadoopFile("D:/002",
    classOf[AvroKey[String]], classOf[NullWritable],
    classOf[AvroKeyOutputFormat[String]], job1.getConfiguration)

  println("job1 done")


  //**************************to generate an avro file based on avro type
  var av = sc.textFile("D://bdp//NewHuman//Users.txt",5)
  var job = Job.getInstance

//  AvroJob.setOutputKeySchema(job, User.getClassSchema)
//  val avArray = av.map(x => x.split(" "))
//  val userP = avArray.map { x => (new AvroKey[User](User.newBuilder().setFavoriteNumber(Integer.parseInt(x(2))).setName(x(0)).setFavoriteColor(x(1)).build()),NullWritable.get()) }
//  var avP = new PairRDDFunctions(userP)
//  avP.saveAsNewAPIHadoopFile("D:/003", classOf[AvroKey[User]], classOf[NullWritable], classOf[AvroKeyOutputFormat[User]], job.getConfiguration)

  println("job2 done")

}
