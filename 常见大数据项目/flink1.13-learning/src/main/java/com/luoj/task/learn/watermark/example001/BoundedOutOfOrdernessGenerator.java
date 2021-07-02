package com.luoj.task.learn.watermark.example001;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple6;

/**
 * This generator generates watermarks assuming that elements arrive out of order,
 * but only to a certain degree. The latest elements for a certain timestamp t will arrive
 * at most n milliseconds after the earliest elements for timestamp t.
 */
@Slf4j
public class BoundedOutOfOrdernessGenerator implements WatermarkGenerator<Tuple6<String, String, String, String, Double, Long>> {

    // 3.5 seconds
    private final long maxOutOfOrderness = 3500;
    private long currentMaxTimestamp;

    @Override
    public void onEvent(Tuple6<String, String, String, String, Double, Long> event, long eventTimestamp, WatermarkOutput output) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
    }

}
