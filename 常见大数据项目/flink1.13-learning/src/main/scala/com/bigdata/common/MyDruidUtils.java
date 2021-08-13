package com.bigdata.common;

import com.alibaba.druid.pool.DruidDataSource;
import java.sql.Connection;

/**
 * @author lj.michale
 * @description  数据库连接池工具类
 * @date 2021-08-13
 */
public class MyDruidUtils {

    private static DruidDataSource dataSource;

    public static Connection getConnection() throws Exception {
        // 使用 Druid 管理链接
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://localhost:3306/test");
        dataSource.setUsername("root");
        dataSource.setPassword("12345678");
        // 初始链接数、最大连接数、最小闲置数
        dataSource.setInitialSize(10);
        dataSource.setMaxActive(50);
        dataSource.setMinIdle(2);
        // 返回链接
        return dataSource.getConnection();
    }
}