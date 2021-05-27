package com.luoj.task.learn.operator.aggregate.example001;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPunctuatedWatermarksAdapter;

import javax.xml.crypto.*;
import javax.xml.crypto.dsig.keyinfo.KeyInfo;

/**
 * @author lj.michale
 * @description
 * 自定义聚合函数类和窗口类，进行商品访问量的统计，根据滑动时间窗口，按照访问量排序输出
 * @date 2021-05-27
 */
public class AggregateFunctionMain {

    /**
     * 滑动窗口大小
     */
    public  static int windowSize=6000;

    /**
     * 滑动窗口滑动间隔
     */
    public  static int windowSlider=3000;

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> sourceData = senv.readTextFile("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\datasets\\ProductViewData2.txt");
//        sourceData.print();

        DataStream<ProductViewData> productViewData = sourceData.map(new MapFunction<String, ProductViewData>() {
            @Override
            public ProductViewData map(String value) throws Exception {
                String[] record = value.split(",");
                return new ProductViewData(record[0], record[1], Long.valueOf(record[2]), Long.valueOf(record[3]));
            }
        }).assignTimestampsAndWatermarks(new WatermarkStrategy<ProductViewData>(){
                    @Override
                    public WatermarkGenerator<ProductViewData> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {

                        return new WatermarkGenerator<ProductViewData>(){

                            private long bound = 3000L;
                            private long maxTimestamp = 0;

                            @Override
                            public void onEvent(ProductViewData productViewData, long eventTimestamp, WatermarkOutput watermarkOutput) {
                                maxTimestamp = Math.max(eventTimestamp, productViewData.getTimestamp());
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                output.emitWatermark(new Watermark(maxTimestamp == Long.MIN_VALUE ? 0 : (maxTimestamp - bound)));
                            }
                        };
                    }
                }
        );

        // 过滤操作类型为1  点击查看的操作
        DataStream<String> productViewCount = productViewData.filter(new FilterFunction<ProductViewData>() {
            @Override
            public boolean filter(ProductViewData productViewData) throws Exception {
                if(Integer.valueOf(productViewData.getOperationType().toString()) == 1){
                    return true;
                }
                return false;
            }
        }).keyBy(new RecordSeclectId())
                .timeWindow(Time.milliseconds(windowSize), Time.milliseconds(windowSlider))
//                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize), Time.milliseconds(windowSlider)))
//                .trigger(ProcessingTimeTrigger.create())
                .aggregate(new MyCountAggregate(), new MyCountWindowFunction());

        productViewCount.print();

        senv.execute("AggregateFunctionMain");
    }


}
