package com.bigdata.task.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.scala.OutputTag;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import scala.runtime.TraitSetter;

public interface BaseFlink {

    StreamExecutionEnvironment env();

    StreamExecutionEnvironment ssc();

    StreamTableEnvironment tableEnv();

    StreamTableEnvironment flink();

    OutputTag outputTag();

    Configuration buildConf(Configuration var1);

    void init(Object var1, String[] var2);

    Object init$default$1();

    String[] init$default$2();

    void createContext(Object var1);

    void deployConf();

    void loadConf();

    void process();
}
