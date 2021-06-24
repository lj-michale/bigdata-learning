package com.luoj.task.learn.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 *  ValueState<T> ：这个状态为每一个 key 保存一个值
 *      value() 获取状态值
 *      update() 更新状态值
 *      clear() 清除状态
 *
 *      IN,输入的数据类型
 *      OUT：数据出的数据类型
 * @author lj.michale
 * @date 2021-06-24
 */
public class CountAverageWithValueState
        extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
    /**
     * 定义一个state：keyed state
     * 1. ValueState里面只能存一条数据，如果来了第二条，就会覆盖第一条。
     * 2. Tuple2<Long, Long>
     *      Long:
     *          当前的key出现多少次  count 3
     *      Long：
     *          当前的value的总和   sum
     *          sum/count = avg
     *  如果我们想要使用这个state，首先要对state进行注册（初始化），固定的套路*
     */
    private ValueState<Tuple2<Long, Long>> countAndSum;

    /**
     * 这个方法其实是一个初始化的方法，只会执行一次
     * 我们可以用来注册我们的状态
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<Tuple2<Long, Long>>(
                        "average",  // 状态的名字
                        Types.TUPLE(Types.LONG, Types.LONG)); // 状态存储的数据类型

        countAndSum = getRuntimeContext().getState(descriptor);
    }

    /**
     * 每来一条数据，都会调用这个方法
     * key相同
     * @param element
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap(Tuple2<Long, Long> element,
                        Collector<Tuple2<Long, Double>> out) throws Exception {
        // 拿到当前的 key 的状态值
        Tuple2<Long, Long> currentState = countAndSum.value();

        // 如果状态值还没有初始化，则初始化
        if (currentState == null) {
            currentState = Tuple2.of(0L, 0L);
        }
        // 更新状态值中的元素的个数 ： count
        currentState.f0 += 1;

        // 更新状态值中的总值
        currentState.f1 += element.f1;
        // 更新状态
        countAndSum.update(currentState);

        // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
        if (currentState.f0 == 3) {
            double avg = (double)currentState.f1 / currentState.f0;
            // 输出 key 及其对应的平均值
            out.collect(Tuple2.of(element.f0, avg));
            //  清空状态值
            countAndSum.clear();
        }
    }
}