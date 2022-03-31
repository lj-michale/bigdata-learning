package com.aurora.feature.datastream;

import com.aurora.feature.func.udf.ReduceWordsStateWindowUDF;
import com.aurora.feature.func.udf.SentenceToWordsUDF;
import com.aurora.feature.func.udf.WordToWordCountUDF;
import com.aurora.generate.SentenceUDSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * @descri 单词计算
 *
 * @author lj.michale
 * @date 2022-03-31
 */
public class WordCountTask {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 加载自定义数据源
        DataStreamSource<String> sentenceDataSource = env.addSource(new SentenceUDSource());

        // 设置水印
        SingleOutputStreamOperator<String> sentenceWithWatermarkStream =
                sentenceDataSource.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((word, ts) -> System.currentTimeMillis()));

        // 将句子分隔为单词
        SingleOutputStreamOperator<String> wordsDataStream = sentenceWithWatermarkStream.flatMap(new SentenceToWordsUDF());
        // 将单词转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCountDataStream = wordsDataStream.map(new WordToWordCountUDF());
        // 按照单词进行分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordCountDataStream.keyBy(t -> t.f0);
        // 5秒滚动时间窗口
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> tumblingWindowedStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        // 用状态进行聚合计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream =
                tumblingWindowedStream.process(new ReduceWordsStateWindowUDF());

        // 打印sink输出
        resultDataStream.print();

        env.execute("WordCount for Unit Testing!");
    }
}