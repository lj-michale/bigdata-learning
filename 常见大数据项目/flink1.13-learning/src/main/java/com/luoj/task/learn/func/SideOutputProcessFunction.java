package com.luoj.task.learn.func;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * 功能描述: 随机进行side-output输出，仅供测试。
 */
public class SideOutputProcessFunction<T> extends ProcessFunction<T, T> {

    private static Logger LOG = LoggerFactory.getLogger(SideOutputProcessFunction.class);

    private OutputTag<T> outputTag;

    public SideOutputProcessFunction(OutputTag<T> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void processElement(T event, Context ctx, Collector<T> out) throws Exception {
        out.collect(event);
        if(new Random().nextInt() % 5 == 0) {
            LOG.warn(String.format("side-output...[%s]", event));
            ctx.output(outputTag, event);
        }
    }

}
