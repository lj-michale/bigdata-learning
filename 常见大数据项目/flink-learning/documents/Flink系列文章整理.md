

###  Flink系列文章整理
<hr></hr>
 
#### 1 概念
* [Flink学习1-基础概念](https://blog.csdn.net/baichoufei90/article/details/82875496)
* [Flink-水位](https://blog.csdn.net/baichoufei90/article/details/108164777)
* [Flink-作业提交流程](https://blog.csdn.net/baichoufei90/article/details/108274922)
* [浅析背压（Back Pressure）机制及其在 Spark & Flink中的实现](https://blog.csdn.net/baichoufei90/article/details/103396623)
* [Java-SPI在Flink中的应用](https://blog.csdn.net/baichoufei90/article/details/101028910)
* [漫画 | flink watermark 一定只能用时间戳衡量？？？](https://mp.weixin.qq.com/s/WEvKjmKPM3OCog0KKV3skA)
* [深入解析 Flink 的算子链机制](https://mp.weixin.qq.com/s/mOdq33TfAYZRFgyTciqqXQ)
* [Flink State 误用之痛，竟然 90% 以上的 Flink 开发都不懂](https://mp.weixin.qq.com/s/0mWNi45_bs2lIC2LsQ_wUA)
* [Flink1.10系列：状态管理解读](https://mp.weixin.qq.com/s/tReSudyKe8nqB0U5A3WXSQ)
* [Flink 1.11 Unaligned Checkpoint 解析](https://mp.weixin.qq.com/s/AThpnvtJsAmI8Fhs8wVuLg)
* [Flink 1.11 新特性详解:【非对齐】Unaligned Checkpoint 优化高反压](https://mp.weixin.qq.com/s/rxxpePoh-Z2fRwQexwsbxg)
* [深入解读 Flink 资源管理机制](https://mp.weixin.qq.com/s/9RHVaeDJGR0WTrEeBapcNw)
* [Flink 端到端 Exactly-once 机制剖析](https://mp.weixin.qq.com/s/fhUNuCOVFQUjRB-fo4Rl2g)
* [一文搞懂 Flink 的 Exactly Once 和 At Least Once](https://ververica.cn/developers/flink-exactly-once-and-at-least-once/)
![image](https://img-blog.csdnimg.cn/20201208234839891.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2JhaWNob3VmZWk5MA==,size_16,color_FFFFFF,t_70)


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

###### 11 应用和实践
###### 11.1 综合
* [Apache Flink在滴滴的应用与实践](https://mp.weixin.qq.com/s/0-eAVizSHQhYe7f_KSFo9Q)
* [bilibili 实时平台的架构与实践](https://mp.weixin.qq.com/s/Y7EyLRYA2U29ElBYATKksA)
* [日均处理万亿数据！Flink在快手的应用实践与技术演进之路](https://mp.weixin.qq.com/s/BghNofoU6cPRn7XfdHR83w)
* [Flink 在有赞实时计算的实践](https://www.toutiao.com/a6648230916705485315/?tt_from=weixin&utm_campaign=client_share&wxshare_count=1&timestamp=1547970368&app=news_article&utm_source=weixin&iid=58124782371&utm_medium=toutiao_android&group_id=6648230916705485315)
###### 11.2 实时数仓
* [实时数据架构，终于有人把他说清楚了！](https://mp.weixin.qq.com/s/CbATFlkjmNvJvarCK9NlgA)
* [ULTRON — 基于Flink的实时数仓平台](https://mp.weixin.qq.com/s/uZA8eacml5urJoKlwHc6oQ)
* [Flink1.11+Hive批流一体数仓](https://mp.weixin.qq.com/s/0mO20FDfBNj7VFJxAsRqeA)
* [生产实践 | Flink 如何建设实时公共画像维表？](https://mp.weixin.qq.com/s/hz8w7fzBzDG73XVwNA_eAA)
* [网易云音乐基于 Flink + Kafka 的实时数仓建设实践](https://mp.weixin.qq.com/s/-Anw03jmXm8LMx6i3qGPbg)
* [进击的 Flink：网易云音乐实时数仓建设实践](https://mp.weixin.qq.com/s/n4RUxDu3PuGBNl6QXNlp4Q)
* [当 TiDB 与 Flink 相结合：高效、易用的实时数仓](https://mp.weixin.qq.com/s/iega5ZItzTChgnIGZhkkjA)
* [基于 Flink + Hive 构建流批一体准实时数仓](https://mp.weixin.qq.com/s/bK289-tcuX9i8v5ZhkiarA)
* [王者荣耀背后的实时大数据平台](https://mp.weixin.qq.com/s/KvY_EcGpDla_ShL7arAD9w)
* [腾讯看点基于 Flink 的实时数仓及多维实时数据分析实践](https://mp.weixin.qq.com/s/k0h2IkAMniHiCJjA1l5Wyg)
* [滴滴基于 Flink 的实时数仓建设实践](https://mp.weixin.qq.com/s/ECi8C_8yRMWQiM-V9C38Rw)
* [基于 Flink 的典型 ETL 场景实现方案](https://mp.weixin.qq.com/s/l--W_GUOGXOWhGdwYqsh9A)
* [字节跳动基于Flink的MQ-Hive实时数据集成](https://mp.weixin.qq.com/s/SDkgYqBZrejObpJ_2bpURw)
* [美团点评 Flink 实时数仓应用经验分享](https://mp.weixin.qq.com/s/QKl4OAd187_PNUFORpBBXA)
* [美团点评基于 Flink 的实时数仓平台实践](https://mp.weixin.qq.com/s/JT6BZzsAM8D8p9F99VmeFw)
* [菜鸟实时数仓技术架构演进](https://mp.weixin.qq.com/s/6Q4zwB1UMl0eAlGQ1LPcPg)
* [知乎实时数仓架构演进](https://mp.weixin.qq.com/s/IBsi0JpU7mm8EOoxl9iayg)
* [小米流式平台|实时数仓架构演进与实践](https://mp.weixin.qq.com/s/anAPTfXRMX-VYa_UYFIKGg)
* [OPPO 数据中台之基石：基于 Flink SQL 构建实数据仓库](https://card.weibo.com/article/m/show/id/2309404371820311672180)
###### 11.3 实时分析
* [生产实践 | Flink + 直播（三）| 如何建设当前正在直播 xx 数？](https://mp.weixin.qq.com/s/NsVYDWk892VONV-XfNGrhw)
* [基于Flink的用户行为日志分析系统](https://mp.weixin.qq.com/s/PDWW3fLbLNESHV6jCIUL6A)
* [Apache Flink OLAP引擎性能优化及应用](https://mp.weixin.qq.com/s/4_ZsIrFNw7APklay5JcMFg)
* [Flink在快手实时多维分析场景的应用](https://mp.weixin.qq.com/s/a7-qQXhiQYxea1wngTtsZw)
* [趣头条基于Flink+ClickHouse的实时数据分析平台](https://mp.weixin.qq.com/s/cF2tDD80Xlto9moUjYtMAw)
* [基于 Flink 的超大规模在线实时反欺诈系统的建设与实践](https://mp.weixin.qq.com/s/72h33s9A8pB3zm56P2zWZw)
###### 11.4 实时监控
* [利用InfluxDB+Grafana搭建Flink on YARN作业监控大屏](https://mp.weixin.qq.com/s/VIzK_VBGI7_sNFe_iL2f3A)
* [从 0 到 1 搭建一套 Flink 的监控系统](https://mp.weixin.qq.com/s/nSpJksVmlnA_7x59okLOGg)
* [Flink全链路延迟的测量方式](https://mp.weixin.qq.com/s/A6CIPsGf-aCWXkB7O-toVw)
###### 11.5 机器学习
* [Flink 在机器学习领域的生产落地](https://mp.weixin.qq.com/s/nn9Z3Sb-yhBHWGx1Ip_zxg)
* [如何用一套引擎搞定机器学习全流程？](https://mp.weixin.qq.com/s/c5bZy_v15FtT1oJGW0UAWQ)
###### 11.6 实时数据湖
* [基于 Flink+Iceberg 构建企业级实时数据湖](https://mp.weixin.qq.com/s/hLw1mDxMDSiRIZXa-mO3sQ)
* [网易：Flink + Iceberg 数据湖探索与实践](https://mp.weixin.qq.com/s/KytPn1qjeN24FKe_2QqkMA)
###### 11.7 实时数据同步
* [基于Binlog与Flink实时同步数据仓库实践](https://mp.weixin.qq.com/s/-gqYuqLrGE5czQhXyYGecg)
* [基于Binlog实时同步数据仓库问题总结](https://mp.weixin.qq.com/s/tgUvfsYIpmvM0kc0yF6lUg)
###### 11.8 优化实践
* [快手基于 Apache Flink 的优化实践](https://mp.weixin.qq.com/s/X1d_fbQxKpFzy87wIgHFdg)
* [Flink RocksDB 状态后端参数调优实践](https://mp.weixin.qq.com/s/YpDi3BV8Me3Ay4hzc0nPQA)
* [如何提高 CPU 利用率？Flink 该如何设定并行度？调大并行度一定会提高 Flink 吞吐吗？](https://mp.weixin.qq.com/s/eUCMc48rX0hyBgV3zhU7mQ)
* [并行度改变引发的血案](https://mp.weixin.qq.com/s/VLzjPsokQii7BMNhFRJFUw)
* [Flink SQL CDC 上线！我们总结了 13 条生产实践经验](https://mp.weixin.qq.com/s/Mfn-fFegb5wzI8BIHhNGvQ)
* [Flink 使用大状态时的一点优化](https://mp.weixin.qq.com/s/mjWGWJVQ_zSVOgZqtjjoLw)
* [Flink State 最佳实践](https://mp.weixin.qq.com/s/ddEuwu-ioz1KdcVHhpIeUg)
* [如何在 Flink 中规划 RocksDB 内存容量？](https://mp.weixin.qq.com/s/ylqK9_SuPKBKoaKmcdMYPA)
* [阿里巴巴大规模应用Flink的踩坑经验：如何大幅降低 HDFS 压力？](https://mp.weixin.qq.com/s/7PAumCJ-RfMcUG7Ean-WnQ)









