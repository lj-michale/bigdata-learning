package com.luoj.task.learn.partition;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.Partitioner;

/**
 * @author lj.michale
 * @description 自定义分区
 * @date 2021-04-10
 */
@Slf4j
public class MyPartition implements Partitioner {

    @Override
    public int partition(Object o, int numPartitions) {
        log.info("分区总数:{}", numPartitions);
        if(Long.valueOf(o.toString()) % 2 == 0){
            return 0;
        } else {
            return 1;
        }
    }
}
