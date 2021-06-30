package com.luoj.common;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author lj.michale
 * @description
 * @date 2021-06-30
 */
public class DruidConnectionUtils {

    private transient static DataSource dataSource = null;
    private final transient static Properties PROPERTIES = new Properties();

    // 静态代码块
    static {
        PROPERTIES.put("driverClassName", "oracle.jdbc.OracleDriver");
        PROPERTIES.put("url", "jdbc:oracle:thin:@localhost:1521:prod");
        PROPERTIES.put("username", "ods");
        PROPERTIES.put("password", "ods");
        try {
            dataSource = DruidDataSourceFactory.createDataSource(PROPERTIES);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private DruidConnectionUtils() {
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}