//package com.aurora.sink;
//
///**
// * @author lj.michale
// * @description
// * @date 2021-08-22
// */
//import com.aurora.bean.Message;
//import com.aurora.common.HBaseUtil;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//
//import java.util.ArrayList;
//import java.util.List;
//
//public class HbaseSink extends RichSinkFunction<Message> {
//
//    private Integer maxSize = 1000;
//    private Long delayTime = 5000L;
//    private String tableName;
//
//    public HbaseSink(String tableName) {
//        this.tableName = tableName;
//    }
//
//    public HbaseSink(Integer maxSize, Long delayTime) {
//        this.maxSize = maxSize;
//        this.delayTime = delayTime;
//    }
//
//    private Connection connection;
//    private Long lastInvokeTime;
//    private List<Put> puts = new ArrayList<Put>();
//
//    // 创建连接
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        HBaseUtil.getHbaseConnection();
//        // 获取系统当前时间
//        lastInvokeTime = System.currentTimeMillis();
//    }
//
//    @Override
//    public void invoke(Message value, Context context) throws Exception {
//
//        System.out.println(value);
//        String rk = value.id+"-"+value.ts;
//        //创建put对象，并赋rk值
//        Put put = new Put(rk.getBytes());
//
//        // 添加值：f1->列族, order->属性名 如age， 第三个->属性值 如25
//        put.addColumn("cf1".getBytes(), "id".getBytes(), value.id.getBytes());
//        put.addColumn("cf1".getBytes(), "vals".getBytes(), value.vals.getBytes());
//        put.addColumn("cf1".getBytes(), "p".getBytes(), (value.p+"").getBytes());
//        put.addColumn("cf1".getBytes(), "ts".getBytes(), (value.ts+"").getBytes());
//        System.out.println("----------");
//        System.out.println(put);
//        // 添加put对象到list集合
//        puts.add(put);
//
//        //使用ProcessingTime
//        long currentTime = System.currentTimeMillis();
//
//        System.out.println(currentTime - lastInvokeTime);
//        //开始批次提交数据
//        if (puts.size() == maxSize || currentTime - lastInvokeTime >= delayTime) {
//
//            //获取一个Hbase表
//            Table table = connection.getTable(TableName.valueOf(tableName));
//            table.put(puts);//批次提交
//
//            puts.clear();
//
//            lastInvokeTime = currentTime;
//            table.close();
//        }
//    }
//
//    @Override
//    public void close() throws Exception {
//        connection.close();
//    }
//
//}