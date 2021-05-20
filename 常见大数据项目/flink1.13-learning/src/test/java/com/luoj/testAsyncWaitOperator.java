package com.luoj;

/**
 * @author lj.michale
 * @description
 * Tests the basic functionality of the AsyncWaitOperator: Processing a limited stream of
 * elements by doubling their value. This is tested in for the ordered and unordered mode.
 * @date 2021-05-20
 */

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class testAsyncWaitOperator {

    @Test
    public void testAsyncWaitOperator() throws Exception {

//        final int numElements = 5;
//        final long timeout = 1000L;
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<Tuple2<Integer, NonSerializable>> input = env.addSource(new NonSerializableTupleSource(numElements));
//
//        AsyncFunction<Tuple2<Integer, NonSerializable>, Integer> function = new RichAsyncFunction<Tuple2<Integer, NonSerializable>, Integer>() {
//
//            private static final long serialVersionUID = 7000343199829487985L;
//
//            transient ExecutorService executorService;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//                executorService = Executors.newFixedThreadPool(numElements);
//            }
//
//            @Override
//            public void close() throws Exception {
//                super.close();
//                executorService.shutdownNow();
//            }
//
//            @Override
//            public void asyncInvoke(final Tuple2<Integer, NonSerializable> input, final AsyncCollector<Integer> collector) throws Exception {
//                executorService.submit(new Runnable() {
//
//                    @Override
//                    public void run() {
//                        collector.collect(Collections.singletonList(input.f0 + input.f0));
//                    }
//                });
//            }
//        };
//
//        DataStream<Integer> orderedResult = AsyncDataStream.orderedWait(input, function, timeout, TimeUnit.MILLISECONDS, 2).setParallelism(1);
//        // save result from ordered process
//        final MemorySinkFunction sinkFunction1 = new MemorySinkFunction(0);
//        final List<Integer> actualResult1 = new ArrayList<>(numElements);
//        MemorySinkFunction.registerCollection(0, actualResult1);
//        orderedResult.addSink(sinkFunction1).setParallelism(1);
//        DataStream<Integer> unorderedResult = AsyncDataStream.unorderedWait(input, function, timeout, TimeUnit.MILLISECONDS, 2);
//        // save result from unordered process
//        final MemorySinkFunction sinkFunction2 = new MemorySinkFunction(1);
//        final List<Integer> actualResult2 = new ArrayList<>(numElements);
//        MemorySinkFunction.registerCollection(1, actualResult2);
//        unorderedResult.addSink(sinkFunction2);
//        Collection<Integer> expected = new ArrayList<>(10);
//        for (int i = 0; i < numElements; i++) {
//            expected.add(i + i);
//        }
//        env.execute();
//        Assert.assertEquals(expected, actualResult1);
//        Collections.sort(actualResult2);
//        Assert.assertEquals(expected, actualResult2);
//        MemorySinkFunction.clear();
    }

}
