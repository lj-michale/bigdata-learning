
Flink 1.12 官方文档：
https://ci.apache.org/projects/flink/flink-docs-release-1.12/
https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/common.html

Flink1.13 官方文档：
http://flink.iteblog.com/dev/windows.html
http://flink.iteblog.com/dev/scala_api_extensions.html

DataStream Transformations
http://flink.iteblog.com/dev/datastream_api.html


对于Hive兼容的表，需要注意数据类型，具体的数据类型对应关系以及注意点如下:
Flink数据类型	    Hive数据类型
CHAR(p)	            CHAR(p)
VARCHAR(p)	        VARCHAR(p)
STRING	            STRING
BOOLEAN	            BOOLEAN
TINYINT	            TINYINT
SMALLINT	        SMALLINT
INT	                INT
BIGINT	            LONG
FLOAT	            FLOAT
DOUBLE	            DOUBLE
DECIMAL(p, s)	    DECIMAL(p, s)
DATE	            DATE
TIMESTAMP(9)	    TIMESTAMP
BYTES	            BINARY
ARRAY<T>	        LIST<T>
MAP<K, V>	        MAP<K, V>
ROW	                STRUCT

Hive CHAR(p) 类型的最大长度为255
Hive VARCHAR(p)类型的最大长度为65535
Hive MAP类型的key仅支持基本类型，而Flink’s MAP 类型的key执行任意类型
Hive不支持联合数据类型，比如STRUCT
Hive’s TIMESTAMP 的精度是 9 ， Hive UDFs函数只能处理 precision <= 9的 TIMESTAMP 值
Hive 不支持 Flink提供的 TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE, 及MULTISET类型
FlinkINTERVAL 类型与 Hive INTERVAL 类型不一样

上面介绍了普通表和Hive兼容表，那么我们该如何使用Hive的语法进行建表呢？这个时候就需要使用Hive Dialect。
什么是Hive Dialect
从Flink1.11.0开始，只要开启了Hive dialect配置，用户就可以使用HiveQL语法，这样我们就可以在Flink中使用Hive的语法使用一些DDL和DML操作。
Flink目前支持两种SQL方言(SQL dialects),分别为：default和hive。默认的SQL方言是default，如果要使用Hive的语法，需要将SQL方言切换到hive。

flink-table-common：通过自定义函数、格式等扩展表生态系统的通用模块。
flink-table-api-java：使用 Java 开发 Table & SQL API 依赖（早期开发阶段，不推荐使用）
flink-table-api-scala：使用 Scala 开发 Table & SQL API 依赖（早期开发阶段，不推荐使用）
flink-table-api-java-bridge：使用 Java，支持 DataStream/DataSet API 的 Table 和 SQL API。
flink-table-api-scala-bridge：使用 Scala，支持 DataStream/DataSet API 的 Table 和 SQL API。
flink-table-planner：Flink 1.9 之前的 planner 和 runtime，仍然可用。
flink-table-planner-blink：新的 Blink planner
flink-table-runtime-blink：新的 Blink runtime
flink-table-uber：将上面的 API 模块以及旧的 planner 打包到 Table 和 SQL API 用例的发行版中。默认情况下，uber JAR文件Flink -table-*. jar 位于Flink发行版的/lib目录中。
flink-table-uber-blink：将上面的 API 模块以及 Blink 特定的模块打包到 Table 和 SQL API 用例的发行版中。默认情况下，uber JAR文件Flink -table-blink-*. jar 位于Flink发行版的/lib目录中。

提交任务
./bin/flink run -d -e kubernetes-session -Dkubernetes.cluster-id=k test.jar

停止 session cluster
echo 'stop' | ./bin/kubernetes-session.sh -Dkubernetes.cluster-id=kaibo-test -Dexecution.attached=true

手工删除资源：
kubectl delete service/<ClusterID>

官方文档 ： Table DataStream 互转
https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/common/
StreamExecutionEnvironment : 流式相关。不能使用SQL的API。如果要在流里面用SQL，使用下面的
StreamTableEnvironment ： 流式SQL相关。可以使用 SQL的API。如果要用Stream相关的，需要将tableData.toRetractStream[Row]

Glossary[注释]
https://ci.apache.org/projects/flink/flink-docs-master/docs/concepts/glossary/


Timely stream processing is an extension of stateful stream processing in which time plays some role in the computation

Flink JIRA
https://issues.apache.org/jira/login.jsp?os_destination=%2Fprojects%2FFLINK

CDH
http://172.17.9.90:7180/cmf/home



