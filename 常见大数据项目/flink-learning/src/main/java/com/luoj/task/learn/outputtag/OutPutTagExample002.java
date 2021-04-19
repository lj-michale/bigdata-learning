package com.luoj.task.learn.outputtag;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.scala.OutputTag;
import org.apache.flink.util.Collector;

/**
 * @author lj.michale
 * @description
 * @date 2021-04-19
 */
public class OutPutTagExample002 {

    public static void main(String[] args) {

        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Integer> ds = env.fromElements(1,2,3,4,5,6,7,8,9,10);

        // 分流器-sideoutput
        OutputTag<Integer> oddTag = new OutputTag<>("奇数", TypeInformation.of(Integer.class));
        OutputTag<Integer> evenTag = new OutputTag<>("偶数", TypeInformation.of(Integer.class));

        SingleOutputStreamOperator<Integer> result = ds.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                // ctx可以将数据放到不同的OutputTag, out收集完的还是放在一起的
                 if(value % 2 == 0){
                     ctx.output(evenTag, value);
                 } else {
                     ctx.output(oddTag, value);
                 }
            }
        });

        DataStream<Integer> oddResult = result.getSideOutput(oddTag);
        DataStream<Integer> evenResult = result.getSideOutput(evenTag);

        oddResult.print("奇数:");
        evenResult.print("偶数:");

    }

}
