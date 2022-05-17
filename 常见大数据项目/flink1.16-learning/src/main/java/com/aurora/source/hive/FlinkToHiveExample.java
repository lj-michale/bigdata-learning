package com.aurora.source.hive;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.hive.HiveModule;

import java.util.Map;
import java.util.Objects;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-05-17
 */
@Slf4j
public class FlinkToHiveExample {

    public static void main(String[] args) {



    }

    /**
     * @descr 创建hiveCatalog
     *
     * @param tEnv
     * @param catalogMap  catalog的设置内容
     * @param setMap 设置语句解析后的k-v表
     *
     */
    private void createHiveCatalogStatement(TableEnvironment tEnv,
                                            Map<String, String> catalogMap,
                                            Map<String, String> setMap) {
        if (catalogMap.size() > 0) {
            HiveCatalog hiveCatalog = new HiveCatalog(
                    catalogMap.get("CATALOG_NAME"),
                    "DEFAULT_HIVE_DATABASE",
                    catalogMap.get("CATALOG_HIVE_CONF"),
                    catalogMap.get("CATALOG_HIVE_VERSION")
            );

            tEnv.registerCatalog(catalogMap.get("CATALOG_NAME"), hiveCatalog);
            tEnv.loadModule(catalogMap.get("CATALOG_NAME"), new HiveModule(catalogMap.get("CATALOG_HIVE_VERSION")));
            tEnv.unloadModule(catalogMap.get("CATALOG_NAME"));
            tEnv.useDatabase("DEFAULT_HIVE_DATABASE");
            log.info("成功创建hive catalog ,默认catalog名称：{}, 默认databases名称为:{}",
                    catalogMap.get("CATALOG_NAME"), "DEFAULT_HIVE_DATABASE");

            if (setMap.containsKey("SQL_DIALECRT")) {
                if (Objects.equals( setMap.get("SQL_DIALECRT").toLowerCase(), "HIVE_DIALECT")) {
                    tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
                    log.info("已切换sql为hive,请注意您的sql语句，只可以写hive表");
                }
            }
        }
    }

}
