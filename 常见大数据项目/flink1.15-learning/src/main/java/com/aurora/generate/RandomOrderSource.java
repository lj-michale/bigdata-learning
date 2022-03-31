package com.aurora.generate;

import com.aurora.bean.Order;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @descri 随机订单
 *
 * @author lj.michale
 * @date 2022-04-01
 */
public class RandomOrderSource extends RichParallelSourceFunction<Order> {

    private boolean flag = true;
    private final Random random = new Random();

    @Override
    public void run(SourceFunction.SourceContext<Order> ctx) throws Exception {
        while (flag){
            TimeUnit.SECONDS.sleep(1);
            String id = UUID.randomUUID().toString();
            Integer userId = random.nextInt(21);
            Integer money = random.nextInt(101);
            long currentTimeMillis = System.currentTimeMillis();
            ctx.collect(new Order(id,userId,money,currentTimeMillis));
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
