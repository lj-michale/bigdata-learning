package com.luoj.task.learn.operator.example003;


import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-29
 */
public class ListStateOperator extends AbstractStreamOperator<Tuple2<Integer, Integer>>
        implements OneInputStreamOperator<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>,
        BoundedOneInput {

    public static OutputTag<String> STATE_RESULT_TAG =
            new OutputTag<String>(Constants.OUTPUT_TAG_NAME) {};

    private final int totalRecords;

    private ListStateDescriptor<Integer> listStateDescriptor;
    private ListState<Integer> listState;

    public ListStateOperator(int totalRecords) {
        this.totalRecords = totalRecords;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.listStateDescriptor = new ListStateDescriptor<>("state", Integer.class);
        listState = getRuntimeContext().getListState(listStateDescriptor);
    }

    @Override
    public void processElement(StreamRecord<Tuple2<Integer, Integer>> streamRecord)
            throws Exception {

        listState.add(streamRecord.getValue().f1);
        output.collect(streamRecord);
    }

    @Override
    public void endInput() throws Exception {
        getKeyedStateBackend()
                .applyToAllKeys(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        listStateDescriptor,
                        (key, value) -> {
                            List<Integer> values = new ArrayList<>();
                            value.get().forEach(values::add);

                            output.collect(
                                    STATE_RESULT_TAG,
                                    new StreamRecord<>(
                                            key + " " + StringUtils.join(value.get(), " ")));
                        });
    }

    public static void checkResult(String fileName, int totalRecords, int numberOfKeys)
            throws IOException {
        Set<Integer> processedKeys = new HashSet<>();

        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
            stream.forEach(
                    line -> {
                        String[] parts = line.split(" +", 2);
                        int key = Integer.parseInt(parts[0]);
                        String value = parts[1];

                        if (processedKeys.contains(key)) {
                            throw new RuntimeException("Repeat keys: " + key);
                        }

                        processedKeys.add(key);

                        int maxValue = totalRecords / numberOfKeys;
                        if (key < totalRecords % numberOfKeys) {
                            maxValue += 1;
                        }

                        String expectedValue =
                                StringUtils.join(
                                        IntStream.range(0, maxValue)
                                                .boxed()
                                                .collect(Collectors.toList()),
                                        " ");
                        if (!value.equals(expectedValue)) {
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