package com.luoj.task.learn.udf;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author lj.michale
 * @description 实现一个将单词转换为元组的UDF
 * @date 2021-07-26
 */
public class WordToWordCountUDF extends RichMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> map(String word) {
        Tuple2<String, Integer> wordCount = Tuple2.of(word, 1);
        return wordCount;
    }
}
