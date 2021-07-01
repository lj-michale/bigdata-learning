package com.luoj.task.learn.accumulator;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author lj.michale
 * @description
 *       ========== Flink累加器 ==========
 *       IntCounter
 *       LongCounter
 *       DoubleCounter
 *       Histogram
 *       自定义(实现SimpleAccumulator接口)
 * @date 2021-07-01
 */
@Slf4j
public class CounterTest {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //--hostname 10.24.14.193  --port 9000
        final ParameterTool params = ParameterTool.fromArgs(args);
        String hostname = params.has("hostname") ? params.get("hostname") : "localhost";
        int port = params.has("port") ? params.getInt("port") : 9000;

        System.out.println("hostName=" + hostname + " port=" + port);

        //数据来源
        DataStream<String> text = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                for (int i = 0; i < 1000; i++) {
                    ctx.collect("t===" + i);
                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() {

            }
        });

        //operate
        text.map(new RichMapFunction<String, String>() {
            //第一步：定义累加器
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //第二步：注册累加器
                getRuntimeContext().addAccumulator("num-lines", this.numLines);
            }

            @Override
            public String map(String s) throws Exception {
                //第三步：累加
                this.numLines.add(1);
                log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>:{}", s);
                return s;
            }
        });

        //数据去向
        text.print();

        //执行
        env.execute("socketTest");
        //JobExecutionResult socketTest = env.execute("socketTest");

        //第四步：结束后输出总量；如果不需要结束后持久化，可以省去，因为在flinkUI中可以看到
        //String total = socketTest.getAccumulatorResult("num-lines").toString();
    }
}