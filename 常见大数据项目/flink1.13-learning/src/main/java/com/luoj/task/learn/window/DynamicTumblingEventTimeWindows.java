package com.luoj.task.learn.window;

import com.bigdata.task.learn.window.DyWindowDemo;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

/**
 * @author lj.michale
 * @description  flink dynamic tumbling event window
 * @date 2021-07-16
 */
@PublicEvolving
public class DynamicTumblingEventTimeWindows extends WindowAssigner<Object, TimeWindow> {

    private static final long serialVersionUID = 1L;

    /**not final, dynamic modify*/
    private long size;

    private long offset;

    protected DynamicTumblingEventTimeWindows() {
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        if (timestamp > Long.MIN_VALUE) {
            Tuple2<DyWindowDemo.DataEntity, DyWindowDemo.Command> element1 = (Tuple2<DyWindowDemo.DataEntity, DyWindowDemo.Command>) element;
            DyWindowDemo.Command command = element1.f1;
            // cal new window size
            // 大于当前时间的情况又怎么处理呢: 窗口开始时间大于 timestamp，下一窗口命令还未开始，数据属于上一窗口命令，所以不修改 size 与 offset
            if (command.startTime() < timestamp) {
                long millis = command.startTime() % 999;
                if ("minute".equalsIgnoreCase(command.periodUnit())) {
                    this.size = command.periodLength() * 60 * 1000;
                    // offset 等于 命令开始时间的 秒值 + 毫秒值
                    long second = command.startTime() / 1000 % 60;
                    offset = second * 1000 + millis;
                } else {
                    this.size = command.periodLength() * 1000;
                    // offset 等于 命令开始时间的 毫秒值
                    offset = millis;
                }
            }
            // todo 窗口开始时间大于或者小于 当前 timestamp 的时候，需要处理
            // 小于当前时间，可以计算出当前timestamp 对应的窗口
            long start = getWindowStartWithOffset(timestamp, offset, size);
            return Collections.singletonList(new TimeWindow(start, start + size));
        } else {
            throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
                    "Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
                    "'DataStream.assignTimestampsAndWatermarks(...)'?");
        }
    }

    /**
     * cal window start time
     */
    public long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        return timestamp - (timestamp + offset - windowSize) % windowSize;
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return EventTimeTrigger.create();
    }

    @Override
    public String toString() {
        return "TumblingEventTimeWindows(" + size + ")";
    }

    /**
     * Creates a new {@code TumblingEventTimeWindows} {@link WindowAssigner} that assigns
     * elements to time windows based on the element timestamp.
     *
     * @return The time policy.
     */
    public static DynamicTumblingEventTimeWindows of() {
        return new DynamicTumblingEventTimeWindows();
    }


    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }

}