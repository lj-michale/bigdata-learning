package com.luoj.example.example1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.*;
import java.util.*;

/**
 * 动态分流的方法
 * 事实表数据由主流输出
 * 维度表数据由侧输出流输出
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {
    // 侧输出流标签
    private OutputTag<JSONObject> dimTag;
    // map状态描述
    private MapStateDescriptor<String,TableProcess> mapStateDescriptor;
    // 定义连接
    private Connection conn;

    public TableProcessFunction(OutputTag<JSONObject> dimTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.dimTag = dimTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        // 创建连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    // 处理业务表中的数据，从kafka里面来的数据
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 1 获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 2 获取表名以及操作类型
        String tableName = jsonObject.getString("table");
        String type = jsonObject.getString("type");

        // 3 如果用maxwell的bootstrap读取历史数据的话，那么操作类型就是bootstrap-insert，进行修复
        if(type.equals("bootstrap-insert")){
            type = "insert";
            jsonObject.put("type",type);
        }
        // 4 从状态中获取key
        String key = tableName +":"+ type;

        // 5 获取广播状态中的配置信息
        TableProcess tableProcess = broadcastState.get(key);

        if(tableProcess != null){
            // 找到了key所对应的配置信息，获取下游类型
            String sinkType = tableProcess.getSinkType();
            // 获取目的地的表
            String sinkTable = tableProcess.getSinkTable();
            // 把表添加到jsonObj中 后面要根据这个信息添加到主题中
            jsonObject.put("sink_table",sinkTable);

            // 过滤字段
            JSONObject dataObj = jsonObject.getJSONObject("data");
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumn(dataObj,sinkColumns);
            // 打印测试
            System.out.println(dataObj.toJSONString());

            if(TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
                // 维度数据 -- 侧输出流中
                ctx.output(dimTag,jsonObject);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                // 事实数据 -- 主流中
                out.collect(jsonObject);
            }
        } else {
            // 没有key所对应的配置信息
            System.out.println(" No this key in TableProcess: " + key);
        }
    }


    //先处理这个流
    //jsonStr  :{"database":"","table":"","type":"","data":{配置表中的一条配置信息}}
    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
        // 1 将字符串转换成对象
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        // 2 从对象中获取data数据
        JSONObject data = jsonObject.getJSONObject("data");
        // 3 将data转换成自定义的tableProcess类
        TableProcess tableProcess = data.toJavaObject(TableProcess.class);
        // 4 获取tableProcess的各个值
        String sourceTable = tableProcess.getSourceTable();
        String operateType = tableProcess.getOperateType();
        String sinkType = tableProcess.getSinkType();
        String sinkTable = tableProcess.getSinkTable();
        String sinkColumns = tableProcess.getSinkColumns();
        String sinkPk = tableProcess.getSinkPk();
        String sinkExtend = tableProcess.getSinkExtend();

        // 5 设定状态主键 表名 + 操作类型
        String key = sourceTable + ":" + operateType;

        // 6 判断当前从配置表中读取到的配置信息是事实还是维度，如果是维度配置，提前将维度表创建出来根据主键添加到状态里面
        if(TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)){
            // 说明是维度表，并且是新添加的表，创建维度表
            checkTable(sinkTable,sinkColumns,sinkPk,sinkExtend);
        }

        // 7 将读取到的一条配置信息放到广播状态中
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(key,tableProcess);

    }

    // 用于检查创建维度表
    public void checkTable(String tableName,String fields,String pk, String ext){
        // 1 空值处理
        if(pk == null){
            pk = "id";
        }
        if(ext == null){
            ext = "";
        }
        // 2 拼接建表语句  注意这里是phoenix
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
        String[] fieldArr = fields.split(",");
        for (int i = 0; i < fieldArr.length; i++) {
            String field = fieldArr[i];
            // 判断是否为主键字段
            if(pk.equals(field)){
                createSql.append(field + " varchar primary key ");
            } else {
                createSql.append(field + " varchar ");
            }
            if(i<fieldArr.length-1){
                createSql.append(",");
            }
        }
        createSql.append(")" + ext);

        // 3 测试打印建表语句
        System.out.println("建表语句" + createSql);

        // 4 获取连接并执行语句
        PreparedStatement ps = null;
        try{
            ps = conn.prepareStatement(createSql.toString());
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("在创建phoenix中创建维度表发生了异常~~");
        } finally {
            // 释放资源
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        //System.out.println("成功-----------------------------");
    }

    public static void filterColumn(JSONObject dataJsonObj,String sinkColumns){
        // 获取要保留的字段，保留的字段在配置表中的sinkColumns列设置
        String[] fieldArr = sinkColumns.split(",");
        // 将数据转换成集合，因为要用到集合中的contains方法
        List<String> fieldList = Arrays.asList(fieldArr);

        // 将集合转换成entrySet对象，用于将不要的属性剔除掉
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        // 将不需要的属性剔除掉
        entrySet.removeIf(entry->!fieldList.contains(entry.getKey()));
    }

}