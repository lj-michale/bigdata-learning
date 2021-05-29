package com.luoj.task.learn.operator.example003.v2;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.PrintWriter;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-29
 */
public class CollectResultSink extends RichSinkFunction<String> {

    private final String fileName;
    private PrintWriter printWriter;

    public CollectResultSink(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.printWriter = new PrintWriter(fileName);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        printWriter.write(value + "\n");
    }

    @Override
    public void close() throws Exception {
        super.close();

        printWriter.close();
    }
}
