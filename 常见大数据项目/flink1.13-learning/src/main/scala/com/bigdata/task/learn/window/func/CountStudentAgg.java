package com.bigdata.task.learn.window.func;


import com.bigdata.bean.Student;
import org.apache.flink.api.common.functions.AggregateFunction;


/**
 * @author lj.michale
 * @description COUNT 统计的聚合函数实现，将结果累加，每遇到一条记录进行加一
 * @date 2021-08-13
 */
public class CountStudentAgg implements AggregateFunction<Student, Long ,Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Student value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}