package com.luoj.task.learn.watermark.example001;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple6;

///**
// * @author lj.michale
// * @description
// * @date 2021-07-01
// */
//public class PunctuatedAssigner implements WatermarkGenerator<Tuple6<String, String, String, String, Double, Long>> {
//
//    @Override
//    public void onEvent(Tuple6<String, String, String, String, Double, Long> event, long eventTimestamp, WatermarkOutput output) {
//        if (event.hasWatermarkMarker()) {
//            output.emitWatermark(new Watermark(event.getWatermarkTimestamp()));
//        }
//    }
//
//    @Override
//    public void onPeriodicEmit(WatermarkOutput output) {
//        // don't need to do anything because we emit in reaction to events above
//    }
//}
