package com.luoj.task.learn.sideoutputs;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * @author lj.michale
 * 1、filter
 * 同一个数据流，遍历多次：
 * 缺点：分几次流就需要，遍历几次原始的数据流，浪费集群资源，影响效率。
 * @description
 * @date 2021-04-27
 */
public class Flink_side_output_filter {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<UserImage> item = env.addSource(new SourceFunction<UserImage>() {
            private Boolean flag = true;
            Random random = new Random();

            @Override
            public void run(SourceContext<UserImage> ctx) throws Exception {
                while (flag) {
                    String orderId = UUID.randomUUID().toString();
                    int userId = random.nextInt(2);
                    int money = random.nextInt(101);
                    // 随机模拟延迟
                    long eventtime = System.currentTimeMillis() - random.nextInt(5) * 1000;
                    ctx.collect(new UserImage(orderId, userId, money, eventtime));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }

        });

        SingleOutputStreamOperator<UserImage> lufei = item.filter((FilterFunction<UserImage>) value -> value.getGroupId() == 1);
        SingleOutputStreamOperator<UserImage> others = item.filter((FilterFunction<UserImage>) value -> value.getGroupId() != 1);

        lufei.printToErr();
        others.print();

        env.execute();

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class UserImage {

        private String orderId;
        private Integer userId;
        private String money;
        private Integer eventtime;
        private Integer groupId;

        public UserImage(String orderId, int userId, int money, long eventtime) { }

    }

}
