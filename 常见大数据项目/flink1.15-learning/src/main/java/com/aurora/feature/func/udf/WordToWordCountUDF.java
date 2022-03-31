package com.aurora.feature.func.udf;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @descri 将单词转换为元组的UDF
 *
 * @author lj.michale
 * @date 2022-03-31
 */
public class WordToWordCountUDF extends RichMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> map(String word) {
        Tuple2<String, Integer> wordCount = Tuple2.of(word, 1);

        return wordCount;
    }
}
