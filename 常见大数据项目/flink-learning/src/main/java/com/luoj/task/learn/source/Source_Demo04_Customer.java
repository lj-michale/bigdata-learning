package com.luoj.task.learn.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * @program flink-demo
 * @description: 演示DataStream-Source-基于自定义数据源
 * @author: lj
 * 2.2.1 随机生成数据
 * API
 * 一般用于学习测试,模拟生成一些数据
 * Flink还提供了数据源接口,我们实现该接口就可以实现自定义数据源，不同的接口有不同的功能，分类如下：
 *
 * SourceFunction:非并行数据源(并行度只能=1)
 * RichSourceFunction:多功能非并行数据源(并行度只能=1)
 * ParallelSourceFunction:并行数据源(并行度能够>=1)
 * RichParallelSourceFunction:多功能并行数据源(并行度能够>=1)–后续学习的Kafka数据源使用的就是该接口
 * 需求
 * 每隔1秒随机生成一条订单信息(订单ID、用户ID、订单金额、时间戳)
 * 要求:
 *
 * 随机生成订单ID(UUID)
 * 随机生成用户ID(0-2)
 * 随机生成订单金额(0-100)
 * 时间戳为当前系统时间
 * ————————————————
 * 版权声明：本文为CSDN博主「erainm」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
 * 原文链接：https://blog.csdn.net/eraining/article/details/114396559
 * @create: 2021/03/02 14:38
 */
public class Source_Demo04_Customer {

    public static void main(String[] args) throws Exception {

        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 2.source
        DataStreamSource<Order> streamSource = env.addSource(new MyOrderSource()).setParallelism(2);
        // 3.transformation

        // 4.sink
        streamSource.print();
        // 5.execute
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order{
        private String id;
        private Integer userId;
        private Integer money;
        private Long createTime;
    }

    public static class MyOrderSource extends RichParallelSourceFunction<Order>{
        private Boolean flag = true;
        /**
         * 执行并生成数据
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            Random random = new Random();

            while (true){
                String oid = UUID.randomUUID().toString();
                int userId = random.nextInt(3);
                int money = random.nextInt(101);
                long createTime = System.currentTimeMillis();
                ctx.collect(new Order(oid,userId,money,createTime));
                Thread.sleep(1000);
            }
        }

        /**
         * 执行cancel命令时执行
         */
        @Override
        public void cancel() {
            flag = false;
        }
    }
}