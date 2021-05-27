package com.luoj.task.learn.statebackends;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.ConfigurableRocksDBOptionsFactory;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.rocksdb.*;

import java.util.Collection;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-27
 */
public class MyOptionsFactory implements ConfigurableRocksDBOptionsFactory {

    private static final long DEFAULT_SIZE = 256 * 1024 * 1024;  // 256 MB
    private long blockCacheSize = DEFAULT_SIZE;

    @Override
    public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return currentOptions.setIncreaseParallelism(4)
                .setUseFsync(false);
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(
            ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return currentOptions.setTableFormatConfig(
                new BlockBasedTableConfig()
                        .setBlockCacheSize(blockCacheSize)
                        .setBlockSize(128 * 1024));            // 128 KB
    }

    @Override
    public RocksDBNativeMetricOptions createNativeMetricsOptions(RocksDBNativeMetricOptions nativeMetricOptions) {
        return null;
    }

    @Override
    public WriteOptions createWriteOptions(WriteOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return null;
    }

    @Override
    public ReadOptions createReadOptions(ReadOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return null;
    }

//    @Override
//    public RocksDBOptionsFactory configure(Configuration configuration) {
//        this.blockCacheSize =
//                configuration.getLong("my.custom.rocksdb.block.cache.size", DEFAULT_SIZE);
//        return this;
//    }

    @Override
    public RocksDBOptionsFactory configure(ReadableConfig readableConfig) {
        return null;
    }
}