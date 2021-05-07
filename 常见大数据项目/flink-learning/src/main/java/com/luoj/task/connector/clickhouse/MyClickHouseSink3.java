package com.luoj.task.connector.clickhouse;


import com.bigdata.common.ClickHouseUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
/**
 * @author lj.michale
 * @description
 * @date 2021-05-07
 */
public class MyClickHouseSink3 extends RichSinkFunction<List<Row>> {
    Connection connection = null;
    String sql;

    //Row 的字段和类型
    private String[] tableColums;
    private  String[] types;

    public MyClickHouseSink3(String sql, String[] tableColums,  String[] types) {
        this.sql = sql;
        this.tableColums = tableColums;
        this.types = types;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = ClickHouseUtil.getConnection("10.1.30.10", 8123, "qinghua");
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }

    @Override
    public void invoke(List<Row> value, Context context) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for (Row e : value) {
            int length = tableColums.length;
            for (int i = 0; i < length; i++) {
                String type = types[i];
                if (e.getField(i) != null) {
                    switch (type) {
                        case "string":
                            preparedStatement.setString(i + 1, (String) e.getField(i));
                            break;
                        case "int":
                            preparedStatement.setInt(i + 1, (int) e.getField(i));
                            break;
                        default:
                            break;
                    }
                } else {
                    preparedStatement.setObject(i + 1, null);
                }
            }
            preparedStatement.addBatch();
        }
        long startTime = System.currentTimeMillis();
        int[] ints = preparedStatement.executeBatch();
        connection.commit();
        long endTime = System.currentTimeMillis();
        System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + ints.length);

    }

}