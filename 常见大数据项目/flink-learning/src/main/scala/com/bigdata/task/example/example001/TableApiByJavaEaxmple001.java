package com.bigdata.task.example.example001;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Random;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-17
 */
@Slf4j
public class TableApiByJavaEaxmple001 {

    private static final int BOUND = 100;

    public static void main(String[] args) throws Exception {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);
//
//        EnvironmentSettings mySetting = EnvironmentSettings
//                .newInstance()
//                .useBlinkPlanner()
//                .inBatchMode()
//                .build();
//        TableEnvironment tableEnv = TableEnvironment.create(mySetting);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings mySetting = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, mySetting);

        DataStream<AddSearchPlanBean> addSearchPlanBeanDataStreamSource = env.addSource(new AddSearchPlanSource());
        // addSearchPlanBeanDataStreamSource.print();

        tableEnv.createTemporaryView("t_SearchPlan", addSearchPlanBeanDataStreamSource);

        String querySQL = "SELECT * FROM t_SearchPlan";

        Table tableSearchPlan = tableEnv.sqlQuery(querySQL);

        DataStream<Row> resultDataStream = tableEnv.toAppendStream(tableSearchPlan, Row.class);
        DataStream<String> resultStream = resultDataStream.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row row) throws Exception {
                String s1 = row.getField(0).toString();
                String s2 = row.getField(1).toString();
                String s3 = row.getField(2).toString();
                String s4 = row.getField(3).toString();
                String s5 = row.getField(4).toString();
                String s6 = row.getField(5).toString();
                return s1+s2+s3+s4+s5+s6;
            }
        });
        resultStream.print();
        env.execute("TableApiByJavaEaxmple001");

    }

    // 自定义AddSearchPlanBean Source数据源
    public static class AddSearchPlanSource implements SourceFunction<AddSearchPlanBean> {

        private static final long serialVersionUID = 1L;
        Random rnd = new Random();
        private volatile boolean isRunning = true;
        private int count = 0;

        @Override
        public void run(SourceContext<AddSearchPlanBean> sourceContext) throws Exception {
            while (isRunning) {
                 int randNum = rnd.nextInt(BOUND/2 -1) + 1;
                 String name = "JG-" + String.valueOf(randNum);
                 long userId = 21346138L;
                 int sourceType = 101;
                 String plan = "Android";
                 Byte defaultFlag = 1;
                 Byte systemFlag = 1;
                 AddSearchPlanBean addSearchPlanBean = new AddSearchPlanBean();
                 addSearchPlanBean.setName(name);
                 addSearchPlanBean.setUserId(userId);
                 addSearchPlanBean.setSourceType(sourceType);
                 addSearchPlanBean.setPlan(plan);
                 addSearchPlanBean.setDefaultFlag(defaultFlag);
                 addSearchPlanBean.setSystemFlag(systemFlag);
                 //log.info(">>>>>>>>>>>addSearchPlanBean:{}", JSON.toJSONString(addSearchPlanBean));
                 sourceContext.collect(addSearchPlanBean);
                 count++;
                 Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
