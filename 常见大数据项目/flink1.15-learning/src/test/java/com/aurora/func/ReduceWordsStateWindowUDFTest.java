//package com.aurora.func;
//
//import com.aurora.feature.func.function.WordCountReduceFunction;
//import com.aurora.feature.func.udf.ReduceWordsStateWindowUDF;
//import org.apache.flink.api.common.ExecutionConfig;
//import org.apache.flink.api.common.state.ReducingStateDescriptor;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
//import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueProcessWindowFunction;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.util.concurrent.ConcurrentLinkedQueue;
//
///**
// * @descri
// *
// * @author lj.michale
// * @date 2022-04-01
// */
//public class ReduceWordsStateWindowUDFTest {
//
//    private KeyedOneInputStreamOperatorTestHarness<String, Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness;
//
//    private static class TupleKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
//        private static final long serialVersionUID = 1L;
//
//        @Override
//        public String getKey(Tuple2<String, Integer> value) throws Exception {
//            return value.f0;
//        }
//    }
//
//    @Before
//    public void setupTestHarness() throws Exception {
//        // 1、构建被测使用到了状态、时间的UDF
//        ReduceWordsStateWindowUDF reduceWordsStateWindowUDF = new ReduceWordsStateWindowUDF();
//
//        // 2. 创建一个WindowState Description
//        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>(
//                "window-contents"
//                , new WordCountReduceFunction()
//                , TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}).createSerializer(new ExecutionConfig()));
//
//        WindowOperator<String
//                        , Tuple2<String, Integer>
//                        , Tuple2<String, Integer>
//                        , Tuple2<String, Integer>
//                        , TimeWindow> operator =
//                new WindowOperator<>(
//                        TumblingEventTimeWindows.of(Time.seconds(5))
//                        , new TimeWindow.Serializer()
//                        , new TupleKeySelector()
//                        , BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig())
//                        , stateDesc
//                        , new InternalSingleValueProcessWindowFunction<
//                                                Tuple2<String, Integer>
//                                                , Tuple2<String, Integer>
//                                                , String
//                                                , TimeWindow>(reduceWordsStateWindowUDF)
//                        , EventTimeTrigger.create()
//                        ,0
//                        , null);
//
//        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(operator
//                , new TupleKeySelector()
//                , BasicTypeInfo.STRING_TYPE_INFO);
//
//        // 4、调用TestHarness对象的open方法（open方法会自动调用我们的open方法实现）
//        testHarness.open();
//    }
//
//    @Test
//    public void process() throws Exception {
//        // 存储期望输出
//        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
//
//        // 用testHarness emit几个数据
//        testHarness.processElement(Tuple2.of("hadoop", 1), 1000);
//        testHarness.processElement(Tuple2.of("spark", 1), 1500);
//        testHarness.processElement(Tuple2.of("hadoop", 1), 2000);
//        testHarness.processElement(Tuple2.of("hadoop", 1), 4999);
//
//        testHarness.processWatermark(new Watermark(4999));
//
//        expectedOutput.add(new StreamRecord<>(Tuple2.of("hadoop", 3), 4999));
//        expectedOutput.add(new StreamRecord<>(Tuple2.of("spark", 1), 4999));
//        // 因为window还会把watermark继续发送到下游，所以watermark也要添加进来
//        expectedOutput.add(new Watermark(4999));
//
//        // 校验结果
//        TestHarnessUtil.assertOutputEqualsSorted("window计算错误", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());
//    }
//
//    private static class Tuple2ResultSortComparator implements Comparator<Object>, Serializable {
//        @Override
//        public int compare(Object o1, Object o2) {
//            if (o1 instanceof Watermark || o2 instanceof Watermark) {
//                return 0;
//            } else {
//                StreamRecord<Tuple2<String, Integer>> sr0 = (StreamRecord<Tuple2<String, Integer>>) o1;
//                StreamRecord<Tuple2<String, Integer>> sr1 = (StreamRecord<Tuple2<String, Integer>>) o2;
//                if (sr0.getTimestamp() != sr1.getTimestamp()) {
//                    return (int) (sr0.getTimestamp() - sr1.getTimestamp());
//                }
//                int comparison = sr0.getValue().f0.compareTo(sr1.getValue().f0);
//                if (comparison != 0) {
//                    return comparison;
//                } else {
//                    return sr0.getValue().f1 - sr1.getValue().f1;
//                }
//            }
//        }
//    }
//}