package com.aurora.feature.func.udf;

import com.aurora.feature.func.function.WordCountReduceFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @descri 对单词进行聚合计算，并将计算结果保存在State中
 *
 * @author lj.michale
 * @date 2022-03-31
 */
@Slf4j
public class ReduceWordsStateWindowUDF extends ProcessWindowFunction<
        Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // 用于保存单词计算结果的状态
    private ReducingState<Tuple2<String, Integer>> reducingState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 创建一个state的description
        ReducingStateDescriptor<Tuple2<String, Integer>> reducingStateDes = new ReducingStateDescriptor<>("wordcount"
                , new WordCountReduceFunction()
                , TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));

        reducingState = getRuntimeContext().getReducingState(reducingStateDes);
    }

    @Override
    public void process(String s
            , Context context
            , Iterable<Tuple2<String, Integer>> elements
            , Collector<Tuple2<String, Integer>> out) {
        // 打印window的计算时间
        TimeWindow window = context.window();
        log.info(sdf.format(window.getEnd()) + " window触发计算！");

        elements.forEach(t -> {
            try {
                if(reducingState != null) {
                    // 将单词直接放入到状态中即可
                    reducingState.add(t);
                    // 将结果继续输出到下游
                    out.collect(reducingState.get());
                }
            } catch (Exception e) {
                log.error("add wordcount tuple to reducing state error!", e);
            }
        });
    }
}