

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


























