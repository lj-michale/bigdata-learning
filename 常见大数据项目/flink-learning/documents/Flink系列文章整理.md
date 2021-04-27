

###  Flink系列文章整理
<hr></hr>
 
#### 1 概念
* Flink学习1-基础概念
* Flink-水位
* Flink-作业提交流程
* 浅析背压（Back Pressure）机制及其在 Spark & Flink中的实现
* Java-SPI在Flink中的应用
* 漫画 | flink watermark 一定只能用时间戳衡量？？？
* 深入解析 Flink 的算子链机制
* Flink State 误用之痛，竟然 90% 以上的 Flink 开发都不懂
* Flink1.10系列：状态管理解读
* Flink 1.11 Unaligned Checkpoint 解析
* Flink 1.11 新特性详解:【非对齐】Unaligned Checkpoint 优化高反压
* 深入解读 Flink 资源管理机制
* Flink 端到端 Exactly-once 机制剖析
* 一文搞懂 Flink 的 Exactly Once 和 At Least Once

#### 2 安装和配置
*  [安装和配置](https://blog.csdn.net/baichoufei90/article/details/82884554)

#### 3 安装和配置
##### 3 使用
###### 3.1 概览
*  [Flink学习3-API介绍](https://blog.csdn.net/baichoufei90/article/details/82891909)

###### 3.2 DataStream
###### 3.2.1 概览
* [Flink-DataStream-HDFSConnector(StreamingFileSink)](https://blog.csdn.net/baichoufei90/article/details/104009350)
* [HDFS租约与Flink StreamingFileSink](https://blog.csdn.net/baichoufei90/article/details/104860008)
* [Flink学习-DataStream-KafkaConnector](https://blog.csdn.net/baichoufei90/article/details/104009237)
###### 3.2.2 原理
[Flink Kafka 端到端 Exactly-Once 分析](https://ververica.cn/developers/flink-kafka-end-to-end-exactly-once-analysis/)
###### 3.2.3 例子
* [Flink-StreamingFileSink-自定义序列化-Parquet批量压缩](https://blog.csdn.net/baichoufei90/article/details/104749504)
* [Flink-使用rowtime且分窗，Connector读取Kafka写入MySQL例子](https://blog.csdn.net/baichoufei90/article/details/102747748)

###### 3.3 Table&Sql API
###### 3.3.1 概念
* [Flink学习4-流式SQL](https://blog.csdn.net/baichoufei90/article/details/101054148)
* [Flink-FilesystemConnector和HiveConnector](https://blog.csdn.net/baichoufei90/article/details/107832306)
* [Flink-流式SQL源码分析](https://blog.csdn.net/baichoufei90/article/details/105633161)
* [Flink-时间窗口源码分析](https://blog.csdn.net/baichoufei90/article/details/105603180)
* [阿里伍翀-flinkSql1.11demo](https://github.com/wuchong/flink-sql-demo)
补链:https://www.cnblogs.com/dingdangzhijia/p/13360889.html
    https://ververica.cn/developers/demo-flink-sql/

###### 3.3.2 原理
* [Flink 原理与实现：Table & SQL API](https://mp.weixin.qq.com/s/Dah2qUQefkBSjWhepCPGUQ)
* [Flink SQL 如何实现数据流的 Join](https://mp.weixin.qq.com/s/5_fino7Iup1qsgjMe45l2g)

######  3.3.3 使用
* [flink sql 去重算法优化](http://apache-flink.147419.n8.nabble.com/flink-sql-td2012.html#a2031)
* [Flink SQL 实战：HBase 的结合应用](https://mp.weixin.qq.com/s/jp-ayqUcGHHhtQppYN3Vuw)
* [Flink 双流 Join 的3种操作示例](https://mp.weixin.qq.com/s/vTAkUrPfO4DW1qOzJ-Zs4A)
* [Flink 使用 broadcast 实现维表或配置的实时更新](https://mp.weixin.qq.com/s/AM7otprJTV7MIJglw_CwiA)

######  4 流平台
* [Flink-流平台调研](https://blog.csdn.net/baichoufei90/article/details/105112922)
* [Flink-Zeppelin On FlinkSql](https://blog.csdn.net/baichoufei90/article/details/105294787)
* [BIGO 实时计算平台建设实践](https://mp.weixin.qq.com/s/-Wt6sroE-IeTQF_nwbv-HA)
* [bilibili 实时平台的架构与实践](https://mp.weixin.qq.com/s/Y7EyLRYA2U29ElBYATKksA)
* [爱奇艺实时计算平台这样做](https://mp.weixin.qq.com/s/IayYmMJaXorjzSVYDMjo1w)
* [汽车之家基于 Flink 的实时 SQL 平台设计思路与实践](https://mp.weixin.qq.com/s/pj2iVvNcQH-4O03nmw0vzg)

######  5 Flink和其他技术
* [HDFS租约与Flink StreamingFileSink](https://blog.csdn.net/baichoufei90/article/details/104860008)
* [Flink-Zeppelin On FlinkSql](https://blog.csdn.net/baichoufei90/article/details/105294787)
* [Flink 如何读取和写入 Clickhouse？](https://mp.weixin.qq.com/s/BhjpGlfJ9WH1M_7ilfw6aQ)
* [Structured Streaming VS Flink](https://mp.weixin.qq.com/s/F7jHlcc-91bUbCNx50hXww)

######  6 常见问题
* [Flink学习6-常见问题](https://blog.csdn.net/baichoufei90/article/details/102718487)
* [在 Flink 算子中使用多线程如何保证不丢数据？](https://mp.weixin.qq.com/s/YWKw8jhTdaDoppkcoYYf7g)

###### 7 新特性
* [Flink 1.10](https://www.jianshu.com/p/475e7fe0fe58)
* [Flink 1.11](https://blog.csdn.net/baichoufei90/article/details/107466523)
* [Flink 1.12](https://mp.weixin.qq.com/s/fKYfzJe37i3DiQft9k7Dow)

###### 8 源码
###### 8.1 源码编译
* [官网-Building Flink from Source](https://ci.apache.org/projects/flink/flink-docs-release-1.10/flinkDev/building.html)
* [Flink 源码解析 —— 源码编译运行](https://www.jianshu.com/p/ac01fb5c98b6)
* [来自阿里大佬zhisheng_blog](https://blog.csdn.net/StephenLu0422/article/details/86699864)
* [Flink源码编译部署Flink](https://blog.csdn.net/m0_37690267/article/details/104212134)
   简单明了
* [Flink 1.10源代码编译,基于Flink release-1.10分支](https://blog.csdn.net/m0_37690267/article/details/104212134)
* mvn clean install -T 2C -Dfast -Dmaven.compile.fork=true -DskipTests -Dscala-2.11快速编译
###### 8.2 源码解析
* [Flink-基于Netty的网络通信](https://blog.csdn.net/yanghua_kobe/article/details/54233945)
* [阅读源码｜Spark 与 Flink 的 RPC 实现](https://mp.weixin.qq.com/s/8G0FzHS1xVOOODOXlZ4Yrw)
* [两个递归彻底搞懂operator chain](https://mp.weixin.qq.com/s/VFEztiseulHvWfBiD0zcng)
###### 8.3 源码二次开发和Bug修复
* [Flink-源码Bug修复和二次开发实践](https://blog.csdn.net/baichoufei90/article/details/111128172)

###### 9 博客
* [Flink官方博客](https://flink.apache.org/blog/)
###### 10 教程
* [Apache Flink 钉钉群直播教程-基础篇](https://ververica.cn/developers/flink-training-course-basics/)
* [Apache Flink 钉钉群直播教程-进阶篇](https://ververica.cn/developers/flink-training-course-advanced/)
* [Apache Flink 钉钉群直播教程-实时数仓篇](https://ververica.cn/developers/flink-training-course-data-warehouse/)
* [Flink 社区最全学习渠道汇总](https://mp.weixin.qq.com/s/JpoGfR7nNdUWD9Al6DeJeg)















