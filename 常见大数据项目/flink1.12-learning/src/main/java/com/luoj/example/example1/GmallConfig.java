package com.luoj.example.example1;

/**
 * 项目配置类
 */
public class GmallConfig {
    // hbase中的空间
    public static final String HBASE_SCHEMA="GMALL2022_REALTIME";
    // phoenix地址
    public static final String PHOENIX_SERVER="jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
}