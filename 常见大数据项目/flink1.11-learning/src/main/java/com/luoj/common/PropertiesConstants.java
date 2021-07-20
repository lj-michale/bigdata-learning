package com.luoj.common;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-02
 */
public class PropertiesConstants {

    public static final String PROPERTIES_FILE_NAME = "/application.properties";
    public static final String STREAM_PARALLELISM = "stream.parallelism";
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";

    //es config
    public static final String ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS = "40";
    public static final String ELASTICSEARCH_HOSTS = "elasticsearch.hosts";
    public static final String STREAM_SINK_PARALLELISM = "1";


}