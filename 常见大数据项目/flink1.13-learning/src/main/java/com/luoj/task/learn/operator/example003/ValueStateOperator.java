package com.luoj.task.learn.operator.example003;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
/**
 * @author lj.michale
 * @description
 * @date 2021-05-29
 */
public class ValueStateOperator extends AbstractStreamOperator<Tuple2<Integer, Integer>>
        implements OneInputStreamOperator<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>,
        BoundedOneInput {

    public static OutputTag<String> STATE_RESULT_TAG =
            new OutputTag<String>(Constants.OUTPUT_TAG_NAME) {};

    private final int totalRecords;
    private ValueState<Integer> valueState;

    public ValueStateOperator(int totalRecords) {
        this.totalRecords = totalRecords;
    }

    @Override
    public void open() throws Exception {
        super.open();

        valueState =
                getRuntimeContext().getState(new ValueStateDescriptor<>("state", Integer.class));
    }

    @Override
    public void processElement(StreamRecord<Tuple2<Integer, Integer>> streamRecord)
            throws Exception {
        Integer current = valueState.value();
        if (current == null) {
            valueState.update(streamRecord.getValue().f1);
        } else {
            valueState.update(current + streamRecord.getValue().f1);
        }

        output.collect(streamRecord);
    }

    @Override
    public void endInput() throws Exception {
        getKeyedStateBackend()
                .applyToAllKeys(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        new ValueStateDescriptor<>("state", Integer.class),
                        (key, value) ->
                                output.collect(
                                        STATE_RESULT_TAG,
                                        new StreamRecord<>(key + " " + value.value())));
    }

    public static void checkResult(String fileName, int totalRecords, int numberOfKeys)
            throws IOException {
        Set<Integer> processedKeys = new HashSet<>();

        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
            stream.forEach(
                    line -> {
                        String[] parts = line.split(" ");
                        int key = Integer.parseInt(parts[0]);
                        int value = Integer.parseInt(parts[1]);

                        if (processedKeys.contains(key)) {
                            throw new RuntimeException("Repeat keys: " + key);
                        }

                        processedKeys.add(key);

                        int maxValue = totalRecords / numberOfKeys;
                        if (key < totalRecords % numberOfKeys) {
                            maxValue += 1;
                        }

                        int expectedValue = maxValue * (maxValue - 1) / 2;
                        if (value != expectedValue) {
                            throw new RuntimeException(
                                    "Value not match: key = "
                                            + key
                                            + " value = "
                                            + value
                                            + ", expected = "
                                            + expectedValue);
                        }
                    });
        }

        for (int i = 0; i < numberOfKeys; ++i) {
            if (!processedKeys.contains(i)) {
                throw new RuntimeException("Key not found: " + i);
            }
        }
    }
}
