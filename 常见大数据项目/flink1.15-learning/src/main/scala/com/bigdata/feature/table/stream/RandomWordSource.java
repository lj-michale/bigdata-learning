package com.bigdata.feature.table.stream;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-04-28
 */
public class RandomWordSource implements SourceFunction<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RandomWordSource.class);
    private volatile boolean isRunning = true;

    private static final String[] words = new String[]{"The", "brown", "fox", "quick", "jump", "sucky", "5dolla", "word","batch"};

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {

            Thread.sleep(300);
            int rnd = (int) (Math.random() * 10 % words.length);
            LOGGER.info("emit word: {}", words[rnd]);

            ctx.collect(words[rnd]);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}