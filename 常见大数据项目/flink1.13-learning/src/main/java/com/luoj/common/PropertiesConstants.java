package com.luoj.common;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-02
 */
public class PropertiesConstants {

    public static  String PROPERTIES_FILE_NAME = "/application-dev-location.properties";

    static {
        // 读取配置文件 application.properties 中的 ykc.profile
//        String profile = PropConfigUtil.getProperties("ykc.profile");
        String profile = "";

        if(profile.equals("dev-location")){
            PROPERTIES_FILE_NAME="/application-dev.properties";
        }
        if(profile.equals("test")){
            PROPERTIES_FILE_NAME="/application-test.properties";
        }

        if(profile.equals("prod")){
            PROPERTIES_FILE_NAME="/application-prod.properties";
        }
    }

    /**
     * flink环境参数
     */
    public static final String STREAM_PARALLELISM = "ykc.flink.stream.parallelism";
    public static final String STREAM_SINK_PARALLELISM = "ykc.flink.stream.sink.parallelism";
    public static final String STREAM_DEFAULT_PARALLELISM = "ykc.flink.stream.default.parallelism";

    public static final String STREAM_CHECKPOINT_ENABLE = "ykc.flink.stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINT_DIR = "ykc.flink.stream.checkpoint.dir";
    public static final String STREAM_CHECKPOINT_TYPE = "ykc.flink.stream.checkpoint.type";
    public static final String STREAM_CHECKPOINT_INTERVAL = "ykc.flink.stream.checkpoint.interval";

    /**
     * kafka参数 消费者
     */
    public static final String KAFKA_BROKERS = "ykc.kafka.brokers";
    public static final String KAFKA_GROUP_ID = "ykc.kafka.groupId";
    public static final String CONSUMER_KAFKA_TOPIC = "ykc.kafka.consumer.topic";

    /**
     * hbase参数
     */
    public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static final String HBASE_CLIENT_RETRIES_NUMBER = "hbase.zookeeper.property.clientPort";
    public static final String HBASE_NAMESPACE = "hbase.namespace";


    /**
     * hdfs 配置参数
     */
    public static final String HDFS_PATH = "hdfs.path";
    public static final String HDFS_PATH_DATE_FORMAT = "hdfs.path.date.format";
    public static final String HDFS_PATH_DATE_ZONE = "hdfs.path.date.zone";



    /**
     * hive参数
     */
    public static final String HIVE_JDBC_URL = "hive.jdbc.url";
    public static final String HIVE_DATABASE = "hive.database";
    public static final String HIVE_LOCATION = "hive.hdfs.location";
    /**
     * impala参数
     */
    public static final String IMPALA_JDBC_URL = "impala.jdbc.url";


    /**
     * mysql参数
     */
    public static final String MYSQL_DRIVER = "ykc.mysql.driver";
    public static final String MYSQL_URL = "ykc.mysql.url";
    public static final String MYSQL_USERNAME = "ykc.mysql.username";
    public static final String MYSQL_PASSWORD = "ykc.mysql.password";


    /**
     * redis的参数
     */
    public static final String REDIS_HOST = "ykc.redis.host";
    public static final String REDIS_PORT = "ykc.redis.port";
    public static final String REDIS_PASSWORD = "ykc.redis.password";
    public static final String REDIS_TIMEOUT = "ykc.redis.timeout";
    public static final String REDIS_DATABASE = "ykc.redis.database";
    public static final String REDIS_MAXIDLE = "ykc.redis.maxidle";
    public static final String REDIS_MINIDLE = "ykc.redis.minidle";
    public static final String REDIS_MAXTOTAL = "ykc.redis.maxtotal";


//    public static final String PROPERTIES_FILE_NAME = "/application.properties";
//    public static final String STREAM_PARALLELISM = "stream.parallelism";
//    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
//    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";

    //es config
    public static final String ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS = "40";
    public static final String ELASTICSEARCH_HOSTS = "elasticsearch.hosts";
//    public static final String STREAM_SINK_PARALLELISM = "1";


}