package com.luoj.task.learn.cep;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Random;
import java.util.UUID;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-19
 */
public class WebMonitorAlertDynamicConf{

    private static final Logger LOG = LoggerFactory.getLogger(WebMonitorAlertDynamicConf.class);

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream ds = env.addSource(new MySource());
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.registerDataStream(
                "log",
                ds,
                "traceid,timestamp,status,restime,proctime.proctime");

        String sql = "select pv,errorcount,round(CAST(errorcount AS DOUBLE)/pv,2) as errorRate," +
                "(starttime + interval '8' hour ) as stime," +
                "(endtime + interval '8' hour ) as etime  " +
                "from (select count(*) as pv," +
                "sum(case when status = 200 then 0 else 1 end) as errorcount, " +
                "TUMBLE_START(proctime,INTERVAL '1' SECOND)  as starttime," +
                "TUMBLE_END(proctime,INTERVAL '1' SECOND)  as endtime  " +
                "from log  group by TUMBLE(proctime,INTERVAL '1' SECOND) )";

        Table table = tenv.sqlQuery(sql);
        DataStream<Result> dataStream = tenv.toAppendStream(table, Result.class);

        MapStateDescriptor<String,Long> confDescriptor = new MapStateDescriptor<>(
                "config-keywords",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO);

        DataStream confStream = env.addSource(new BroadcastSource());

        BroadcastStream<Integer> broadcastStream = confStream.broadcast(confDescriptor);

        DataStream resultStream = dataStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<Result,Integer,Result>(){
                    @Override
                    public void processElement(
                            Result element,
                            ReadOnlyContext ctx,
                            Collector<Result> out) throws Exception{
                        Long v = ctx.getBroadcastState(confDescriptor)
                                .get("value");
                        if (v != null && element.getErrorcount() > v){
                            LOG.info("收到了一个大于阈值{}的结果{}.", v, element);
                            out.collect(element);
                        }
                    }

                    @Override
                    public void processBroadcastElement(
                            Integer value,
                            Context ctx,
                            Collector<Result> out) throws Exception{
                        ctx.getBroadcastState(confDescriptor)
                                .put("value", value.longValue());

                    }
                });

        env.execute("FlinkDynamicConf");
    }

    public static class BroadcastSource implements SourceFunction<Integer>{

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception{
            while (true){
                Thread.sleep(3000);
                ctx.collect(randInt(15, 20));
            }
        }
        /**
         * 生成指定范围内的随机数
         * @param min
         * @param max
         * @return
         */
        private int randInt(int min, int max){
            Random rand = new Random();
            int randomNum = rand.nextInt((max - min) + 1) + min;
            return randomNum;
        }
        @Override
        public void cancel(){

        }
    }

    public static class MySource implements SourceFunction<Tuple4<String,Long,Integer,Integer>>{

        static int status[] = {200, 404, 500, 501, 301};

        @Override
        public void run(SourceContext<Tuple4<String,Long,Integer,Integer>> sourceContext) throws Exception{
            while (true){
                Thread.sleep((int) (Math.random() * 100));
                // traceid,timestamp,status,response time

                Tuple4 log = Tuple4.of(
                        UUID.randomUUID().toString(),
                        System.currentTimeMillis(),
                        status[(int) (Math.random() * 4)],
                        (int) (Math.random() * 100));

                sourceContext.collect(log);
            }
        }

        @Override
        public void cancel(){

        }
    }

    public static class Result{
        private long pv;
        private int errorcount;
        private double errorRate;
        private Timestamp stime;
        private Timestamp etime;

        public long getPv(){
            return pv;
        }

        public void setPv(long pv){
            this.pv = pv;
        }

        public int getErrorcount(){
            return errorcount;
        }

        public void setErrorcount(int errorcount){
            this.errorcount = errorcount;
        }

        public double getErrorRate(){
            return errorRate;
        }

        public void setErrorRate(double errorRate){
            this.errorRate = errorRate;
        }

        public Timestamp getStime(){
            return stime;
        }

        public void setStime(Timestamp stime){
            this.stime = stime;
        }

        public Timestamp getEtime(){
            return etime;
        }

        public void setEtime(Timestamp etime){
            this.etime = etime;
        }

        @Override
        public String toString(){
            return "Result{" +
                    "pv=" + pv +
                    ", errorcount=" + errorcount +
                    ", errorRate=" + errorRate +
                    ", stime=" + stime +
                    ", etime=" + etime +
                    '}';
        }
    }

}
