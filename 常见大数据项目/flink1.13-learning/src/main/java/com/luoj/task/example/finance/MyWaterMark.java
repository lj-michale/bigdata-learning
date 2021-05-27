package com.luoj.task.example.finance;

import org.apache.flink.api.common.eventtime.*;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-27
 */
public class MyWaterMark implements WatermarkStrategy<Transaction> {

    @Override
    public WatermarkGenerator<Transaction> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {

        return new WatermarkGenerator<Transaction>() {

            private long bound = 3000L;
            private long maxTimestamp = 0;

            @Override
            public void onEvent(Transaction event, long eventTimestamp, WatermarkOutput output) {
                maxTimestamp = Math.max(eventTimestamp, event.getTimestamp());
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {
                output.emitWatermark(new Watermark(maxTimestamp == Long.MIN_VALUE ? 0 : (maxTimestamp - bound)));
            }
        };
    }
}
