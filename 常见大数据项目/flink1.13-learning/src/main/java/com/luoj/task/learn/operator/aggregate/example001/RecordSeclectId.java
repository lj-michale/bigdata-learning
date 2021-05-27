package com.luoj.task.learn.operator.aggregate.example001;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-27
 */
public class RecordSeclectId implements KeySelector<ProductViewData, String> {
    private static final long serialVersionUID = 4780234853172462378L;

    @Override
    public String getKey(ProductViewData value) throws Exception {
        return value.getUserId();
    }
}