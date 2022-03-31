package com.aurora;

import com.aurora.generate.FakeTrafficRecordSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @descri FakeTrafficRecordSourceUnitTest
 *
 * @author lj.michale
 * @date 2022-03-31
 */
public class FakeTrafficRecordSourceUnitTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FakeTrafficRecordSource fakeTrafficRecordSource = new FakeTrafficRecordSource();

        env.addSource(fakeTrafficRecordSource).print();

        env.execute();
    }


}
