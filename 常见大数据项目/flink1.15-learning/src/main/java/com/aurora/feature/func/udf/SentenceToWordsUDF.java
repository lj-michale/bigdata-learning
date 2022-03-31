package com.aurora.feature.func.udf;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @descri 将句子转换为单词
 *
 * @author lj.michale
 * @date 2022-03-31
 */
@Slf4j
public class SentenceToWordsUDF extends RichFlatMapFunction<String, String> {

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
                    } else {
                        log.warn("Word is empty!");
                    }
                }
            } else {
                log.error("The sentence is invalid!\nThe sentence is:" + sentence);
            }
        } else {
            log.warn("Sentence is empty!");
        }
    }
}