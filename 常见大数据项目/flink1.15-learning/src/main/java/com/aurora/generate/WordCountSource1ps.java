package com.aurora.generate;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Random;

/**
 * @descri Flink的WordCount数据源，每秒产生1条数据
 *
 * @author lj.michale
 * @date 2022-03-31
 */
@Slf4j
public class WordCountSource1ps implements SourceFunction<String> {

    private boolean needRun = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (needRun){
            ArrayList<String> result = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                result.add("zhiyong"+i);
            }
            sourceContext.collect(result.get(new Random().nextInt(20)));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        needRun = false;
    }
}
