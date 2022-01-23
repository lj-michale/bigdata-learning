//package com.aurora.cdc;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import com.aliyun.odps.Odps;
//import com.aliyun.odps.account.Account;
//import com.aliyun.odps.account.AliyunAccount;
//import com.aliyun.odps.data.Record;
//import com.aliyun.odps.tunnel.TableTunnel;
//import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
//import com.ververica.cdc.debezium.DebeziumSourceFunction;
//import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.util.Collector;
//
//import java.util.ArrayList;
//import java.util.List;
//
//@Slf4j
//public class PGSourceDemo {
//    public static void main(String[] args) throws Exception {
//        DebeziumSourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
//                .hostname("localhost")
//                .port(5432)
//                .database("postgres")
//                .schemaList("public")
//                .tableList("public.products")
//                .username("postgres")
//                .password("postgres")
//                .deserializer(new JsonDebeziumDeserializationSchema())
//                .build();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5 * 60 * 1000);
//
//        env.addSource(sourceFunction).map(t -> {
//            JSONObject jsonObject = JSON.parseObject(t);
//            JSONObject after = jsonObject.getJSONObject("after");
//            return Product.of(after.toJSONString());
//        })/*.addSink(new SinkFunction<>() {
//            @Override
//            public void invoke(Product value, Context context) throws Exception {
//
//            }
//        }).setParallelism(8);*/
//                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())
//                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(1)))
//                .process(new ProcessAllWindowFunction<Product, List<Product>, TimeWindow>() {
//                    @Override
//                    public void process(ProcessAllWindowFunction<Product, List<Product>, TimeWindow>.Context context, Iterable<Product> elements, Collector<List<Product>> out) throws Exception {
//                        List<Product> res = new ArrayList<>();
//                        for (Product element : elements) {
//                            res.add(element);
//                        }
//                        out.collect(res);
//                    }
//                })
//                .addSink(new RichSinkFunction<>() {
//                    TableTunnel.StreamUploadSession uploadSession;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        /*Properties config = new Properties();
//                        config.put("access_id", "LTAI5t5ze1yHPa96y7n65DDt");
//                        config.put("access_key", "3J6JvGpNiOByEwanHYXiyTdrnUzhuY");
//                        config.put("project_name", "ninja_mc");
//                        config.put("charset", "utf-8");
//                        Connection conn = DriverManager.getConnection("jdbc:odps:http://service.cn-beijing.maxcompute.aliyun.com/api", config);
//                        ps = conn.prepareStatement("insert into products values (?,?)");*/
//                        Account account = new AliyunAccount("LTAI5t5ze1yHPa96y7n65DDt", "3J6JvGpNiOByEwanHYXiyTdrnUzhuY");
//                        Odps odps = new Odps(account);
//                        odps.setEndpoint("http://service.cn-beijing.maxcompute.aliyun.com/api");
//                        odps.setDefaultProject("ninja_mc");
//                        TableTunnel tunnel = new TableTunnel(odps);
//                        uploadSession = tunnel.createStreamUploadSession("ninja_mc", "products");
//                    }
//
//                    @Override
//                    public void invoke(List<Product> values, Context context) throws Exception {
//                        TableTunnel.StreamRecordPack recordPack = uploadSession.newRecordPack();
//                        for (Product value : values) {
//                            Record record = uploadSession.newRecord();
//                            record.setString(0, value.getName());
//                            record.setString(1, value.getDescription());
//                            recordPack.append(record);
//                        }
//                        recordPack.flush();
//                    }
//                });
//
//        env.execute();
//    }
//}