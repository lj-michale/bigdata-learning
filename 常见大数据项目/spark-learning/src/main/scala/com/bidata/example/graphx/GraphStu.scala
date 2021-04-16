package com.bidata.example.graphx

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
/**
 * @Author
 * @Date 2020/11/23
 * @Description   用的最多的就是模式匹配和柯里化  Graph算法：PageRank
 */
object GraphStu {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("GraphStu").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    // 创建 vertices 顶点rdd
    val vertices = sc.makeRDD(Seq((1L, 1), (2L, 2), (3L, 3)))
    // 创建 edges 边rdd
    val edges = sc.makeRDD(Seq(Edge(1L, 2L, 1), Edge(2L, 3L, 2)))
    // 创建 graph对象
    val graph = Graph(vertices, edges)
    // 获取graph图对象的vertices信息
    //    graph.vertices.collect.foreach(println)
    //     获取graph图对象的edges信息
    //    graph.edges.collect.foreach(println)
    // 获取graph图对象的triplets信息
    //    graph.triplets.collect.foreach(println)

    // 通过文件加载
    val graphLoad = GraphLoader.edgeListFile(sc, "E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\spark-learning\\data\\user.txt")
    //    graphLoad.vertices.collect.foreach(println)
    //    graphLoad.edges.collect.foreach(println)
    //    graphLoad.triplets.collect.foreach(println)

    //案例一  user relation
    val users = sc.parallelize(
      Array
      (
        (3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prodessor")),
        (2L, ("istoica", "professor"))
      )
    )

    val relationship = sc.parallelize(
      Array(Edge(3L, 7L, "Colla"),
        Edge(5L, 3L, "Advisor"),
        Edge(2L, 5L, "Colleague"),
        Edge(5L, 7L, "Pi")
      )
    )

    val graphUser = Graph(users, relationship)
    //graphUser.vertices.collect.foreach(println)
    //    graphUser.edges.collect.foreach(println)
    //    graphUser.triplets.collect.foreach(println)


    // ////////////////////////////////////
    //案例二
    //println("--------------案例二-------------------")
    val userRDD = sc.makeRDD(Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    ))

    val userCallRDD = sc.makeRDD(Array(
      Edge(2L, 1L, 7),
      Edge(3L, 2L, 4),
      Edge(4L, 1L, 1),
      Edge(2L, 4L, 2),
      Edge(5L, 3L, 5),
      Edge(3L, 6L, 3),
      Edge(5L, 6L, 3)
    ))

    val userCallGraph = Graph(userRDD,userCallRDD)

    //userCallGraph.vertices.collect.foreach(println)
    //userCallGraph.edges.collect.foreach(println)
    userCallGraph.triplets.collect.foreach(println)

    //userCallGraph.vertices.filter{case(id,(name,age))=>age>30}.collect.foreach(println)

    //获取名字，年龄
    userCallGraph.vertices.filter{case(id,(name,age))=>age>30}.collect.foreach(x=>{println("name: "+x._2._1+" age: "+x._2._2)})

    //userCallGraph.triplets.collect.foreach(println)

    //使用srcAttr,dstAttr,attr获取triplets的指定格式的信息：Bob like Alice stage:7
    userCallGraph.triplets.collect.foreach(x=>println(x.srcAttr._1+" like "+x.dstAttr._1+" stage:"+x.attr))


    //模式匹配 mapVertices  变换结构
    val t1_graph = userCallGraph.mapVertices{case(vertexId,(name,age))=>(vertexId,name)}
    //t1_graph.vertices.collect.foreach(println)
    //map遍历方式  (上下两句效果相同)
    val t2_graph = userCallGraph.mapVertices((id,attr)=>(id,attr._1))
    //t2_graph.vertices.collect.foreach(println)

    //mapEdges 变换结构
    val t3_graph = userCallGraph.mapEdges(e=>Edge(e.srcId,e.dstId,e.attr*7.0))
    val t31_graph = userCallGraph.mapEdges(e=>e.attr*7.0)
    //    t3_graph.edges.collect.foreach(println)
    //    t31_graph.edges.collect.foreach(println)

    //反转
    val reverseUserCallGraph = userCallGraph.reverse
    //reverseUserCallGraph.triplets.collect.foreach(println)

    //println("------------------------------")
    //截取年龄小于65岁的  vp方法
    userCallGraph.subgraph(vpred = (id,attr)=>attr._2<65).triplets.collect.foreach(println)
    println("-------------------------------")
    //截取年龄小于65岁的   ep 方法
    userCallGraph.subgraph(epred = (ep)=>ep.srcAttr._2<65).triplets.collect.foreach(println)

    val two=sc.makeRDD(Array((1L,"kgc.cn"),(2L,"qq.com"),(3L,"163.com")))

    val three=sc.makeRDD(Array((1L,"kgc.cn"),(2L,"qq.com"),(3L,"163.com"),(7L,"sohu.com")))

    //左外连接  使用了柯里化
    // (id,v,cmpy)中的id是关联条件，v是userCallGraph的除了id的值(数据)，cmpy是被关联的two里除了id的数据(邮箱后缀)
    //userCallGraph.joinVertices(two)((id,v,cmpy)=>(v._1+"@"+cmpy,v._2)).triplets.collect.foreach(println)

    //样例类
    case class User(name:String,age:Int,inDeg:Int,outDeg:Int)

    //外连接 使用了柯里化  找出入度最多的人就是被点赞最多的人 id是关联条件
    // (id,u,indeg)中的u是userCallGraph的除了id的值(数据),indeg代表被关联的 userCallGraph的inDegrees(入度数)
    val userOuterjoin
    = userCallGraph.outerJoinVertices(userCallGraph.inDegrees){case(id,u,indeg)=>User(u._1,u._2,indeg.getOrElse(0),0)}

    userOuterjoin.vertices.collect.foreach(println)

    //接着上面 入度和出度都找出来了
    val inDegAndoutDeg
    = userOuterjoin.outerJoinVertices(userCallGraph.outDegrees){case(id,u,outdeg)=>(u.name,u.age,u.inDeg,outdeg.getOrElse(0))}

    inDegAndoutDeg.vertices.collect.foreach(println)

    //for加上模式匹配 获取到用户的id, 姓名,年龄,入度数,出度数
    //最终得出每个人被多少人喜欢(粉丝数)
    for((id,property)<-inDegAndoutDeg.vertices.collect)
      println(s"User $id is ${property._1} and is liked by ${property._3} people.")

    println("--------------------------")

    ///////////
    //pageRank算法： 收敛时允许的误差，越小越精确  随机重置率
    userCallGraph.pageRank(0.0001).vertices.sortBy(x=>x._2,false).collect.foreach(println)
    //输出结果：
    //(1,0.4668633984374999)
    //(6,0.30459375)
    //(4,0.25235859374999997)
    //(2,0.24084375)
    //(3,0.21375)
    //(5,0.15)

    userCallGraph.pageRank(0.5).vertices.sortBy(x=>x._2,false).collect.foreach(println)
    //输出结果：
    //(1,0.15)
    //(2,0.15)
    //(3,0.15)
    //(4,0.15)
    //(5,0.15)
    //(6,0.15)

  }

}
