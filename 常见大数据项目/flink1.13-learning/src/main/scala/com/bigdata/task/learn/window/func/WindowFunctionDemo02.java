package com.bigdata.task.learn.window.func;

import com.bigdata.bean.Student;
import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

/**
 * @author lj.michale
 * @description  Flink1.13之前的写法
 * @date 2021-08-13
 */
public class WindowFunctionDemo02 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设定按照【事件发生】的时间进行处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Student> collectionSource = env.fromCollection(getCollection());

        collectionSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Student>() {
            @Override
            public long extractAscendingTimestamp(Student element) {
                return element.getSuccessTimeStamp();
            }
        }).keyBy("id").timeWindow(Time.milliseconds(100), Time.milliseconds(10))
                .aggregate(new CountStudentAgg(), new WindowStudentResultFunction());



    }


    private static List<Student> getCollection() {
        return Lists.newArrayList(
                Student.of(1, "第一种商品名字 1", 0, "test", 1569640890385L),
                Student.of(2, "第一种商品名字 2",0, "test", 1569640890386L),
                Student.of(3, "第一种商品名字 3",0, "test", 1569640890387L),
                Student.of(4, "第一种商品名字 4",0, "test", 1569640890388L),
                Student.of(5, "第一种商品名字 5",0, "test", 1569640890389L),
                Student.of(6, "第一种商品名字 6",0, "test", 1569640890390L),
                Student.of(7, "第一种商品名字 7",0, "test", 1569640890391L),
                Student.of(8, "第一种商品名字 8",0, "test", 1569640890392L),
                Student.of(9, "第一种商品名字 9",0, "test", 1569640890393L),
                Student.of(10, "第一种商品名字 10", 0, "test", 1569640890394L),
                Student.of(11, "第一种商品名字 11", 0, "test", 1569640890395L),
                Student.of(12, "第一种商品名字 12", 0, "test", 1569640890396L),
                Student.of(13, "第一种商品名字 13", 0, "test", 1569640890397L),
                Student.of(14, "第一种商品名字 14", 0, "test", 1569640890398L),
                Student.of(15, "第一种商品名字 15", 0, "test", 1569640890399L),
                Student.of(16, "第一种商品名字 16", 0, "test", 1569640890400L),
                Student.of(17, "第一种商品名字 17", 0, "test", 1569640890401L),

                Student.of(1, "第二种商品名字 1", 0, "test", 1569640890401L),
                Student.of(2, "第二种商品名字 2", 0, "test",1569640890402L),
                Student.of(3, "第二种商品名字 3", 0, "test",1569640890403L),
                Student.of(4, "第二种商品名字 4", 0, "test",1569640890404L),
                Student.of(5, "第二种商品名字 5", 0, "test",1569640890405L),
                Student.of(6, "第二种商品名字 6", 0, "test",1569640890406L),
                Student.of(7, "第二种商品名字 7", 0, "test",1569640890407L),
                Student.of(8, "第二种商品名字 8", 0, "test",1569640890408L),
                Student.of(9, "第二种商品名字 9", 0, "test",1569640890409L),
                Student.of(10, "第二种商品名字 10", 0, "test", 1569640890410L),
                Student.of(11, "第二种商品名字 11", 0, "test", 1569640890411L),
                Student.of(12, "第二种商品名字 12", 0, "test", 1569640890412L),
                Student.of(13, "第二种商品名字 13", 0, "test", 1569640890413L),
                Student.of(14, "第二种商品名字 14", 0, "test", 1569640890414L),
                Student.of(15, "第二种商品名字 15", 0, "test", 1569640890415L),
                Student.of(16, "第二种商品名字 16", 0, "test", 1569640890416L),
                Student.of(17, "第二种商品名字 17", 0, "test", 1569640890417L),

                Student.of(1, "第三种商品名字 1", 0, "test", 1569640890418L),
                Student.of(2, "第三种商品名字 2", 0, "test",1569640890419L),
                Student.of(3, "第三种商品名字 3", 0, "test",1569640890420L),
                Student.of(4, "第三种商品名字 4", 0, "test",1569640890421L),
                Student.of(5, "第三种商品名字 5", 0, "test",1569640890422L),
                Student.of(6, "第三种商品名字 6", 0, "test",1569640890423L),
                Student.of(7, "第三种商品名字 7", 0, "test",1569640890424L),
                Student.of(8, "第三种商品名字 8", 0, "test",1569640890425L),
                Student.of(9, "第三种商品名字 9", 0, "test",1569640890426L),
                Student.of(10, "第三种商品名字 10", 0, "test", 1569640890427L),
                Student.of(11, "第三种商品名字 11", 0, "test", 1569640890428L),
                Student.of(12, "第三种商品名字 12", 0, "test", 1569640890429L),
                Student.of(13, "第三种商品名字 13", 0, "test", 1569640890430L),
                Student.of(14, "第三种商品名字 14", 0, "test", 1569640890431L),
                Student.of(15, "第三种商品名字 15", 0, "test", 1569640890432L),
                Student.of(16, "第三种商品名字 16", 0, "test", 1569640890433L),
                Student.of(17, "第三种商品名字 17", 0, "test", 1569640890434L)
        );
    }
}
