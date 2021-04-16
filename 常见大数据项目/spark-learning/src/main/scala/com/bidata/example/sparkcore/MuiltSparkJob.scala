package com.bidata.example.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.Future
import java.util
import java.util.concurrent.{Callable, Executors}

import scala.collection.JavaConverters._

// https://blog.csdn.net/u010454030/article/details/74353886?utm_medium=distribute.pc_relevant_t0.none-task-blog-2%7Edefault%7EBlogCommendFromMachineLearnPai2%7Edefault-1.control&dist_request_id=1331978.127.16185672603667071&depth_1-utm_source=distribute.pc_relevant_t0.none-task-blog-2%7Edefault%7EBlogCommendFromMachineLearnPai2%7Edefault-1.control
object MuiltSparkJob {

  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf()
    //实例化spark context
    val sc=new SparkContext(sparkConf)
    sparkConf.setAppName("multi task submit ")

    //保存任务返回值
    val list:util.ArrayList[java.util.concurrent.Future[String]] = new util.ArrayList[java.util.concurrent.Future[String]]()

    //并行任务读取的path
    val task_paths=new util.ArrayList[String]()
    task_paths.add("/tmp/data/path1/")
    task_paths.add("/tmp/data/path2/")
    task_paths.add("/tmp/data/path3/")

    //线程数等于path的数量
    val nums_threads=task_paths.size()

    //构建线程池
    val executors=Executors.newFixedThreadPool(nums_threads)

    for(i<-0 until  nums_threads){
      val task:java.util.concurrent.Future[String]= executors.submit(new Callable[String] {
        override def call(): String ={
          val count=sc.textFile(task_paths.get(i)).count()//获取统计文件数量
          task_paths.get(i)+" 文件数量： "+count
        }
      })
      list.add(task)//添加集合里面
    }

    //遍历获取结果
    list.asScala.foreach(result=>{
      result.get()
    })

    //停止spark
    sc.stop()

  }

}
