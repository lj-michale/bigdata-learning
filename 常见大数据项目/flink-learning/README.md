
flink1.12.1

Flink 1.12 官方文档：
https://ci.apache.org/projects/flink/flink-docs-release-1.12/

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








