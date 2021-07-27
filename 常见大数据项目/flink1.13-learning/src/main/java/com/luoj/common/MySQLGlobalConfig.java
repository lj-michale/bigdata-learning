package com.luoj.common;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-27
 */
public class MySQLGlobalConfig {

    public static final String MySQL_DB = "";

    public static final  String MySQL_URL = "jdbc:mysql://localhost:3306/"+ MySQL_DB +"?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC";

    public static final String MySQL_NAME = "root";

    public static final String MySQL_PASSWORD = "abc1314520";

    public static final String MySQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";

}
