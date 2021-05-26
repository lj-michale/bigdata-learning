package com.luoj.task.learn.operator.join;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Properties;

/**
 * @author lj.michale
 * @description
 * 举例: leftElementEventTime=2019-11-16 17:30:00,lowerBound= -10minute，upperBound=0，则这条leftElement按Key和[2019-11-16 17:20:00,2019-11-16 17:30:00]时间范围内的rightElement join。
 * Interval Join目前只支持Event Time。
 * 数据量比较大，可使用RocksDBStateBackend。
 * 每个用户的点击Join这个用户最近10分钟内的浏览
 *
 * // 点击记录Topic
 * {"userID": "user_2", "eventTime": "2019-11-16 17:30:00", "eventType": "click", "pageID": "page_1"}
 *
 * // 浏览记录Topic
 * {"userID": "user_2", "eventTime": "2019-11-16 17:19:00", "eventType": "browse", "productID": "product_1", "productPrice": 10}
 * {"userID": "user_2", "eventTime": "2019-11-16 17:20:00", "eventType": "browse", "productID": "product_1", "productPrice": 10}
 * {"userID": "user_2", "eventTime": "2019-11-16 17:22:00", "eventType": "browse", "productID": "product_1", "productPrice": 10}
 * {"userID": "user_2", "eventTime": "2019-11-16 17:26:00", "eventType": "browse", "productID": "product_1", "productPrice": 10}
 * {"userID": "user_2", "eventTime": "2019-11-16 17:30:00", "eventType": "browse", "productID": "product_1", "productPrice": 10}
 * {"userID": "user_2", "eventTime": "2019-11-16 17:31:00", "eventType": "browse", "productID": "product_1", "productPrice": 10}
 *
 *
 * @date 2021-05-26
 */
@Slf4j
public class IntervalJoinExample001 {

    public static void main(String[] args) throws Exception{

        args=new String[]{"--application","flink/src/main/java/com/bigdata/flink/dataStreamWindowJoin/application.properties"};

        //1、解析命令行参数
        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(fromArgs.getRequired("application"));

        String kafkaBootstrapServers = parameterTool.getRequired("kafkaBootstrapServers");

        String browseTopic = parameterTool.getRequired("browseTopic");
        String browseTopicGroupID = parameterTool.getRequired("browseTopicGroupID");

        String clickTopic = parameterTool.getRequired("clickTopic");
        String clickTopicGroupID = parameterTool.getRequired("clickTopicGroupID");

        //2、设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //3、添加Kafka数据源
        // 浏览流
        Properties browseProperties = new Properties();
        browseProperties.put("bootstrap.servers",kafkaBootstrapServers);
        browseProperties.put("group.id",browseTopicGroupID);
        DataStream<UserBrowseLog> browseStream=env
                .addSource(new FlinkKafkaConsumer010<>(browseTopic, new SimpleStringSchema(), browseProperties))
                .process(new BrowseKafkaProcessFunction())
                .assignTimestampsAndWatermarks(new BrowseBoundedOutOfOrdernessTimestampExtractor(Time.seconds(0)));

        // 点击流
        Properties clickProperties = new Properties();
        clickProperties.put("bootstrap.servers",kafkaBootstrapServers);
        clickProperties.put("group.id",clickTopicGroupID);
        DataStream<UserClickLog> clickStream = env
                .addSource(new FlinkKafkaConsumer010<>(clickTopic, new SimpleStringSchema(), clickProperties))
                .process(new ClickKafkaProcessFunction())
                .assignTimestampsAndWatermarks(new ClickBoundedOutOfOrdernessTimestampExtractor(Time.seconds(0)));

        //browseStream.print();
        //clickStream.print();

        //4、Interval Join
        //每个用户的点击Join这个用户最近10分钟内的浏览
        clickStream
                .keyBy("userID")
                .intervalJoin(browseStream.keyBy("userID"))
                // 时间间隔,设定下界和上界
                // 下界: 10分钟前，上界: 当前EventTime时刻
                .between(Time.minutes(-10),Time.seconds(0))
                // 自定义ProcessJoinFunction 处理Join到的元素
                .process(new ProcessJoinFunction<UserClickLog, UserBrowseLog, String>() {
                    @Override
                    public void processElement(UserClickLog left, UserBrowseLog right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(left +" =Interval Join=> "+right);
                    }
                })
                .print();

        env.execute();

    }

    /**
     * 解析Kafka数据
     */
    static class BrowseKafkaProcessFunction extends ProcessFunction<String, UserBrowseLog> {
        @Override
        public void processElement(String value, Context ctx, Collector<UserBrowseLog> out) throws Exception {
            try {
                UserBrowseLog log = JSON.parseObject(value, UserBrowseLog.class);
                if(log!=null){
                    out.collect(log);
                }
            }catch (Exception ex){
                log.error("解析Kafka数据异常...",ex);
            }
        }
    }

    /**
     * 解析Kafka数据
     */
    static class ClickKafkaProcessFunction extends ProcessFunction<String, UserClickLog> {
        @Override
        public void processElement(String value, Context ctx, Collector<UserClickLog> out) throws Exception {
            try {
                UserClickLog log = JSON.parseObject(value, UserClickLog.class);
                if(log!=null){
                    out.collect(log);
                }
            }catch (Exception ex){
                log.error("解析Kafka数据异常...",ex);
            }
        }
    }

    /**
     * 提取时间戳生成水印
     */
    static class BrowseBoundedOutOfOrdernessTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserBrowseLog> {

        BrowseBoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(UserBrowseLog element) {
            DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            DateTime dateTime = DateTime.parse(element.getEventTime(), dateTimeFormatter);
            return dateTime.getMillis();
        }
    }

    /**
     * 提取时间戳生成水印
     */
    static class ClickBoundedOutOfOrdernessTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserClickLog> {

        ClickBoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(UserClickLog element) {
            DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            DateTime dateTime = DateTime.parse(element.getEventTime(), dateTimeFormatter);
            return dateTime.getMillis();
        }
    }


}
