package com.aurora.feature.window;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;


/**
 * @descri 自定义窗口实现window
 *         这里主要有两个问题：
 *         1，数据被分到几个窗口？窗口的长度 / 窗口滑动的步长 = 窗口的个数。
 *         2，窗口的开始时间和结束时间怎么计算？对应的 TimeWindow#getWindowStartWithOffset 方法。
 *
 * @author lj.michale
 * @date 2022-04-14
 */
@Slf4j
public class MyEventTimeWindow extends WindowAssigner<Object, TimeWindow> {

    // 窗口的大小
    private final long size;
    // 多长时间滑动一次
    private final long slide;
    // 窗口偏移量
    private final long offset;

    protected MyEventTimeWindow(long size, long slide, long offset) {
        this.size = size;
        this.slide = slide;
        this.offset = offset;
    }

    public static MyEventTimeWindow of(Time size, Time slide, Time offset) {
        return new MyEventTimeWindow(size.toMilliseconds(), slide.toMilliseconds(), offset.toMilliseconds());
    }

    public static MyEventTimeWindow of(Time size, Time slide) {
        return new MyEventTimeWindow(size.toMilliseconds(), slide.toMilliseconds(), 0L);
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext windowAssignerContext) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        // 设置从每天的0点开始计算
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        // 获取窗口的开始时间 其实就是 0 点
        long winStart = calendar.getTimeInMillis();
        // 获取窗口的结束时间,就是在开始时间的基础上加上窗口的长度 这里是 1 天
        calendar.add(Calendar.DATE, 1);
        // 获取窗口的结束时间 其实就是第二天的 0 点
        long winEnd = calendar.getTimeInMillis() + 1;
        String format = String.format("window的开始时间:%s,window的结束时间:%s", winStart, winEnd);
        System.out.println(format);
        // 当前数据所属窗口的结束时间
        long currentWindowEnd = TimeWindow.getWindowStartWithOffset(timestamp, this.offset, this.slide) + slide;
        System.out.println(TimeWindow.getWindowStartWithOffset(timestamp, this.offset, this.slide) + "====" + currentWindowEnd);
        // 一条数据属于几个窗口 因为是滑动窗口一条数据会分配到多个窗口里
        int windowCounts = (int) ((winEnd - currentWindowEnd) / slide);
        List<TimeWindow> windows = new ArrayList<>(windowCounts);
        long currentEnd = currentWindowEnd;
        if (timestamp > Long.MIN_VALUE) {
            while (currentEnd < winEnd) {
                windows.add(new TimeWindow(winStart, currentEnd));
                currentEnd += slide;
            }
        }
        return windows;
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
        return EventTimeTrigger.create();
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
