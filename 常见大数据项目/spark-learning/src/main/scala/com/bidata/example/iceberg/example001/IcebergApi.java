//package com.bidata.example.iceberg.example001;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.iceberg.Schema;
//import org.apache.iceberg.Table;
//import org.apache.iceberg.catalog.Catalog;
//import org.apache.iceberg.catalog.TableIdentifier;
//import org.apache.iceberg.hive.HiveCatalog;
//import org.apache.iceberg.types.Types;
//import org.apache.spark.SparkConf;
//import org.apache.spark.sql.SparkSession;
//
///**
// * @author lj.michale
// * @description
// * @date 2021-04-16
// */
//public class IcebergApi {
//
//    public static Configuration getProperties() {
//
//        System.out.println("start:-----");
//
//        SparkSession spark = SparkSession.builder().config(new SparkConf().setAppName("IcebergApi")
//        ).enableHiveSupport().getOrCreate();
//        System.out.println("spark: " + spark);
//
//        Configuration conf = spark.sparkContext().hadoopConfiguration();
////        conf1.set("spark.sql.warehouse.dir", "/user/bigdata/hive/warehouse/");
//        conf.set("hive.metastore.warehouse.dir", "/user/bigdata/hive/warehouse/");
//
//        return conf;
//    }
//
//    public static Table createTable() {
//        Configuration conf = getProperties();
//        Catalog catalog = new HiveCatalog(conf);
//        System.out.println("catalog: " + catalog);
//
//        TableIdentifier name = TableIdentifier.of("testdb", "ice_table2");
//        System.out.println("name: " + name);
//        Schema schema = new Schema(
//                Types.NestedField.required(1, "level", Types.StringType.get()),
//                Types.NestedField.required(2, "event_time", Types.StringType.get())
//        );
//
//        System.out.println("schema: " + schema);
//        Table table = catalog.createTable(name, schema);
//        System.out.println("end:-----" + table);
//        return table;
//    }
//
//    public static void main( String[] args ) {
//        createTable();
//    }
//
//}
