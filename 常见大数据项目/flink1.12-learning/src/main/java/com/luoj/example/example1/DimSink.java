package com.luoj.example.example1;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * 通过JDBC的方式连接phoenix插入数据
 */
public class DimSink extends RichSinkFunction<JSONObject> {
    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        //TODO 1 获取驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //TODO 2 获取连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    // 来一条数据触发一次
    // 维度表的数据
//    {
//        "database": "gmall2022",
//            "xid": 96203,
//            "data": {
//        "tm_name": "cc",
//                "id": 13
//    },
//        "commit": true,
//            "sink_table": "dwd_base_trademark",
//            "type": "insert",
//            "table": "base_trademark",
//            "ts": 1642495710
//    }
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {
        // 获取输出目的地
        String sinkTable = jsonObject.getString("sink_table");
        // 获取受影响的数据
        JSONObject dataJsonObj = jsonObject.getJSONObject("data");

        //TODO 3 拼接语句
        String upsertSql = getUpsertSql(sinkTable, dataJsonObj);

        //TODO 4 获取执行对象
        PreparedStatement ps = null;
        //TODO 5 执行
        try {
            ps = conn.prepareStatement(upsertSql);
            ps.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("向phoenix表中插入数据发生异常");
        } finally {
            //TODO 6 释放资源
            if (conn != null) {
                conn.close();
            }
        }
    }

    // 拼接语句
    // upsert into 命名空间.表名(id,name) values(1,zhangsan)
    public String getUpsertSql(String sinkTable, JSONObject dataJsonObj) {
        // 获取所有的列名
        Set<String> keys = dataJsonObj.keySet();
        // 获取列名对应的value值
        Collection<Object> values = dataJsonObj.values();

        String upsertSQL = "upsert into " + GmallConfig.HBASE_SCHEMA + "." +
                sinkTable + "(" + StringUtils.join(keys, ",") + ") " +
                "values('" + StringUtils.join(values, "','") + "')";
        return upsertSQL;
    }
}