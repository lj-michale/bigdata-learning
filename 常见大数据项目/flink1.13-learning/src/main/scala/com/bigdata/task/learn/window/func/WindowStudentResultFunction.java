package com.bigdata.task.learn.window.func;


import com.luoj.bean.StudentViewCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author lj.michale
 * @description 用于输出统计学生的结果
 * @date 2021-08-13
 */
public class WindowStudentResultFunction implements WindowFunction<Long, StudentViewCount, Tuple, TimeWindow> {


    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<StudentViewCount> out) throws Exception {
        int id = ((Tuple1<Integer>) tuple).f0;
        long count = input.iterator().next();
        out.collect(StudentViewCount.of(id, window.getEnd(), count));
    }
}