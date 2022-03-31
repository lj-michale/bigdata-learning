package com.aurora.generate;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-03-31
 */
public class SentenceUDSource extends RichSourceFunction<String> {

    private volatile Boolean isCanceled;
    private Random random;

    private static final String[] SENTENCE = {
            "The cat stretched"
            , "Jacob stood on his tiptoes"
            , "The car turned the corner"
            , "Kelly twirled in circles"
            , "She opened the door"
            , "Aaron made a picture"
            , "I am sorry"
            , "I danced"
    };

    @Override
    public void open(Configuration parameters) throws Exception {
        this.isCanceled = false;
        this.random = new Random();
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        while(!isCanceled) {
            int ramdomIndex = random.nextInt(SENTENCE.length);
            ctx.collect(SENTENCE[ramdomIndex]);
            TimeUnit.MILLISECONDS.sleep(500);
        }
    }

    @Override
    public void cancel() {
        this.isCanceled = true;
    }
}