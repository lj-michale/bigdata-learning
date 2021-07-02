package com.luoj.task.learn.watermark.example001;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple6;

/**
 * This generator generates watermarks that are lagging behind processing time
 * by a fixed amount. It assumes that elements arrive in Flink after a bounded delay.
 */
@Slf4j
public class TimeLagWatermarkGenerator implements WatermarkGenerator<Tuple6<String, String, String, String, Double, Long>> {

    // 5 seconds
    private final long maxTimeLag = 5000;

    @Override
    public void onEvent(Tuple6<String, String, String, String, Double, Long> event, long eventTimestamp, WatermarkOutput output) {
        // don't need to do anything because we work on processing time
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
    }

}
