package com.aurora.func;

import com.aurora.feature.func.udf.SentenceToWordsUDF;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-04-01
 */
public class SentenceToWordsUDFTest {

    @Test
    public void flatMap() {
        SentenceToWordsUDF sentenceToWordsUDF = new SentenceToWordsUDF();

        // 使用Mockito mock一个Collector对象
        Collector collector = Mockito.mock(Collector.class);
        sentenceToWordsUDF.flatMap("hadoop spark hello", collector);

        // 因为此处flatMap UDF会输出3次，所以我们用Mockito verify三次，验证每次收集到的单词
        Mockito.verify(collector, Mockito.times(1)).collect("hadoop");
        Mockito.verify(collector, Mockito.times(1)).collect("spark");
        Mockito.verify(collector, Mockito.times(1)).collect("hello");

    }

}
