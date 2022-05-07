package com.aurora.feature.hive;

import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.hive.HiveModule;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-04-30
 */
public class FlinkTableAPIFromHive {

    public static void main(String[] args) {

        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2、创建HiveCatalog
        //hive连接实例
        String name = "myCatalog";

        //hive中的数据库名称
        String defaultDatabase = "test";
        //配置文件hive-site.xml存放在项目中的data/etc/目录
        String hiveConfDir     = "data/etc/";
        //加载Hive Module(可以使用hive的UDF)
        tEnv.loadModule(name, new HiveModule("2.3.6"));
        //使用hive方言(hivesql特有的语法)
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        //3、注册myCatalog
        tEnv.registerCatalog(name, hive);
        //4、设置当前sesion使用的catalog和database
        tEnv.useCatalog(name);
        tEnv.useDatabase(defaultDatabase);
        //5、查询hive中的表
        tEnv.executeSql("select * from clicklog").print();

    }

}