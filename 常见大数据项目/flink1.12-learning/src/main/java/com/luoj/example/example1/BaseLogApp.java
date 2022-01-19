package com.luoj.example.example1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

public class BaseLogApp {

    public static void main(String[] args) throws Exception {

        //TODO 0 构建基本环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 1 设置默认并行度
        env.setParallelism(4);
        //TODO 2 设置检查点

        // 1 开启检查点
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        // 2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 3 设置取消job后，检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 4 设置重启策略  重试次数，延迟时间，意味失败之后，不会立即重新执行 固定延迟重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        // 5 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall/flink/checkpoint"));
        // 6 设置操作用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 3 从kafka读取数据

        //指定消费者信息
        String groupid = "ods_dwd_base_log_app";
        String topic = "ods_base_log";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupid);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        //kafkaDS.print("输出：");

        //TODO 4 转换成json对象

        // 1 匿名内部类的形式
//        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(
//                new MapFunction<String, JSONObject>() {
//                    @Override
//                    public JSONObject map(String s) throws Exception {
//                        JSONObject jsonObject = JSON.parseObject(s);
//                        return jsonObject;
//                    }
//                }
//        );

        // 2 函数默认调用
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);



        //TODO 5 识别新老顾客

        // 1、 按照mid进行分组
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(
                data -> data.getJSONObject("common").getString("mid")
        );

        // 2、 检测是不是新老访客
        SingleOutputStreamOperator<JSONObject> jsonObjWithIsnewDS = midKeyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    // 声明最近访问日期状态
                    private ValueState<String> lastVisitDataState;
                    // 声明格式化对象
                    private SimpleDateFormat simpleDateFormat;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVisitDataState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>(
                                        "lastVisitData",
                                        String.class
                                )
                        );
                        simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        // 打印数据
                        System.out.println(jsonObject);
                        // 获取访问标记 1代表新访客，0代表老访客
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");

                        // 判断然后进行修复
                        if ("1".equals(isNew)) {
                            // 获取最后一次登录状态
                            String lastVisitDate = lastVisitDataState.value();
                            // 获取当前访问日期
                            String currVisitDate = simpleDateFormat.format(new Date(jsonObject.getLong("ts")));

                            // 如果访客状态不为空，则代表已经访问过，则标记置为0
                            if (lastVisitDate != null && lastVisitDate.length() > 0) {
                                if (!lastVisitDate.equals(currVisitDate)) {
                                    isNew = "0";
                                    jsonObject.getJSONObject("common").put("is_new", isNew);
                                }
                            } else {
                                //没有访问过，更新状态
                                lastVisitDataState.update(currVisitDate);
                            }
                        }
                        return jsonObject;
                    }
                }
        );

        //TODO 6 分流

        // 1 定义侧输出流
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};

        // 2 利用侧输出流进行操作
        SingleOutputStreamOperator<String> pageDS = jsonObjWithIsnewDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, Context context, Collector<String> out) throws Exception {
                        //获取启动属性
                        JSONObject startJsonObj = jsonObject.getJSONObject("start");
                        if (startJsonObj != null && startJsonObj.size() > 0) {
                            context.output(startTag, jsonObject.toJSONString());
                        } else {
                            // 启动日志，其他都是页面日志 放到主流当中
                            out.collect(jsonObject.toJSONString());

                            // 在页面日志上，判断是否有曝光行为
                            JSONArray displayArr = jsonObject.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                // 获取时间戳
                                Long ts = jsonObject.getLong("ts");
                                // 获取页面信息
                                String pageId = jsonObject.getJSONObject("page").getString("page_id");


                                // 将曝光数据放到曝光侧输出流中
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject displayJsonObj = displayArr.getJSONObject(i);

                                    // 补充曝光时间以及页面id
                                    displayJsonObj.put("ts", ts);
                                    displayJsonObj.put("page_id", pageId);
                                    context.output(displayTag, displayJsonObj.toJSONString());

                                }
                            }
                        }
                    }
                }
        );

        // 3 将个输出流的数据拿出来
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        // 4 打印各个流的数据
        pageDS.print("页面流");
        startDS.print("启动流");
        displayDS.print("曝光流");

        // 5 发送到不同的kafka主题中
        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        //TODO 执行
        env.execute();
    }
}