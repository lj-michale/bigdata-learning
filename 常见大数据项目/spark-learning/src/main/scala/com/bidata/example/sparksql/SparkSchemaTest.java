package com.bidata.example.sparksql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author lj.michale
 * @description
 * @date 2021-04-16
 */
public class SparkSchemaTest {

    public static void main(String[] args) {

        SparkSession spark=getSparkSession();

        List<LogInfo> data=data();
        List<Row> rowData=new ArrayList<Row>();

        for(LogInfo i:data) {
            List<Row> appinfo=new ArrayList<Row>();
            for(AppInfo j:i.getAppInfo()) {
                Row row=RowFactory.create(j.getLogInfo(),j.getLogTime(),j.getMsg());
                appinfo.add(row);
            }
            Row row=RowFactory.create(i.getLogFilePath(),appinfo);
            rowData.add(row);
        }

        StructType schema=schema();
        Dataset<Row> ds=spark.createDataFrame(rowData, schema);

        ds.printSchema();
        ds.show();
        spark.close();
    }

    public static StructType schema() {
        StructType schema=new StructType();
        schema=schema.add("logFilePath", DataTypes.StringType);


        List<StructField> sf=new ArrayList<StructField>();
        sf.add(new StructField("logTime",DataTypes.StringType,false,null));
        sf.add(new StructField("msg",DataTypes.StringType,false,null));
        sf.add(new StructField("logInfo",DataTypes.StringType,false,null));

        schema=schema.add("appInfo", DataTypes.createArrayType(DataTypes.createStructType(sf)));
        return schema;
    }

    public static List<LogInfo> data(){
        List<LogInfo> data=new ArrayList<LogInfo>();
        LogInfo logInfo=new LogInfo();
        logInfo.setLogFilePath("A");
        ArrayList<AppInfo> appList=new ArrayList<AppInfo>();
        AppInfo appInfo=new AppInfo();
        appInfo.createData(1);
        appList.add(appInfo);
        appInfo=new AppInfo();
        appInfo.createData(2);
        appList.add(appInfo);
        logInfo.setAppInfo(appList);
        data.add(logInfo);

        logInfo=new LogInfo();
        logInfo.setLogFilePath("B");
        appList=new ArrayList<AppInfo>();
        appInfo=new AppInfo();
        appInfo.createData(3);
        appList.add(appInfo);
        appInfo=new AppInfo();
        appInfo.createData(4);
        appList.add(appInfo);
        logInfo.setAppInfo(appList);

        data.add(logInfo);
        return data;
    }

    public static SparkSession getSparkSession() {
        SparkConf conf = new SparkConf();
        conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false");
        conf.setMaster("local[1]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        return spark;
    }
}