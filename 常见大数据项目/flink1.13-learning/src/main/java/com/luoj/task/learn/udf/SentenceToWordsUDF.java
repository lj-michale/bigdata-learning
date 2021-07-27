package com.luoj.task.learn.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author lj.michale
 * @description 将句子转换为单词
 * @date 2021-07-26
 */

public class SentenceToWordsUDF extends RichFlatMapFunction<String, String> {

    private Logger logger = LoggerFactory.getLogger(SentenceToWordsUDF.class);

    @Override
    public void flatMap(String sentence, Collector<String> out) {
        // 将单词用空格切分
        if(!StringUtils.isEmpty(sentence)) {
            String[] words = sentence.split(" ");
            if(words != null && words.length > 0) {
                for (String word : words) {
                    // 继续判断单词是否为空
                    if(StringUtils.isNotEmpty(word)) {
                        out.collect(word);
                    }
                    else {
                        logger.warn("Word is empty!");
                    }
                }
            }
            else {
                logger.error("The sentence is invalid!\nThe sentence is:" + sentence);
            }
        }
        else {
            logger.warn("Sentence is empty!");
        }
    }
}