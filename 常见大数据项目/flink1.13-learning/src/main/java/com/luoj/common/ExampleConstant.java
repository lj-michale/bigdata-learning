package com.luoj.common;

/**
 * @author lj.michale
 * @description
 * @date 2021-06-30
 */
public class ExampleConstant {

    /** iceConfigFile */
    public static String JCACHE_ICE_CONFIG_FILE = "/home/push/javaenv/testenv/bigdatapikatest/jcache_ice_client.properties";

    /** iceGridName */
//    public static String JCACHE_ICE_LOCATOR = "jCacheIceGrid/Locator:tcp -h 172.16.105.4 -p 4061";
    public static String JCACHE_ICE_LOCATOR = "jCacheIceGrid/Locator:tcp -h 172.17.8.17 -p 4061";

    /** proxyName */
//    public static String JCACHE_PROXY_NAME = "jCache";
    public static String JCACHE_PROXY_NAME = "jCacheTest";

    /** redis集群资源名称 */
//    public static String JCACHE_CLUSTER_RESOURCE_NAME = "jCacheTestCluster";
    public static String JCACHE_CLUSTER_RESOURCE_NAME = "segClusterRes";

    /** redis单例资源名称 */
//    public static String JCACHE_SINGLE_RESOURCE_NAME = "jCacheTestStandAlone";
    public static String JCACHE_SINGLE_RESOURCE_NAME = "segSingleRes";

}
