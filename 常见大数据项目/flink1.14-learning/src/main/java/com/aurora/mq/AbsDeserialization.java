package com.aurora.mq;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.metrics.Counter;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-21
 */
public abstract class AbsDeserialization<T> extends AbstractDeserializationSchema<T> {

    private RuntimeContext runtimeContext;

    private String DIRTY_DATA_NAME="dirtyDataNum";

    private String NORMAL_DATA_NAME="normalDataNum";

    /**脏数据*/
    protected transient Counter dirtyDataNum;

    /**正常数据*/
    protected transient Counter normalDataNum;

    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    public void setRuntimeContext(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    public void initMetric() {
        dirtyDataNum=runtimeContext.getMetricGroup().counter(DIRTY_DATA_NAME);
        normalDataNum=runtimeContext.getMetricGroup().counter(NORMAL_DATA_NAME);
    }
}