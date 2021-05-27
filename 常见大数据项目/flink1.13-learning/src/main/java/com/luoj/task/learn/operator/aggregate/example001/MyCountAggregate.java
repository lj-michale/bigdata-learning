package com.luoj.task.learn.operator.aggregate.example001;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author lj.michale
 * @description 自定义聚合函数MyCountAggregate
 *              输入类型(IN)、累加器类型(ACC)和输出类型(OUT)
 * @date 2021-05-27
 */
public class MyCountAggregate implements AggregateFunction<ProductViewData, Long, Long> {

    /*访问量初始化为0*/
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    /*访问量直接+1 即可*/
    @Override
    public Long add(ProductViewData productViewData, Long accumulator) {
        return accumulator + 1;
    }

    /*合并两个统计量*/
    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }


}
