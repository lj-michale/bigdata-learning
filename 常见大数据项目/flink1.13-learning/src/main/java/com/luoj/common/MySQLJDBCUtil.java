package com.luoj.common;

import java.sql.*;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-27
 */
public class MySQLJDBCUtil {

    /**
     * 只注册一次，静态代码块
     */
    static{
        try {
            Class.forName(MySQLGlobalConfig.MySQL_DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接方法
     */
    public static Connection getConnection(){
        try {
            Connection conn = DriverManager.getConnection(MySQLGlobalConfig.MySQL_URL, MySQLGlobalConfig.MySQL_NAME, MySQLGlobalConfig.MySQL_PASSWORD);
            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * 释放资源的方法
     */
    public static void close(Statement stmt, Connection conn){
        if(stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 释放资源的方法
     */
    public static void close(ResultSet rs, Statement stmt, Connection conn){
        if(rs!=null){
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        if(stmt!=null){
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

}
