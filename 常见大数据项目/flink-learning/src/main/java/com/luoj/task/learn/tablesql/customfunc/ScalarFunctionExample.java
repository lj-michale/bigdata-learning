package com.luoj.task.learn.tablesql.customfunc;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lj.michale
 * @description FlinkSQL-自定义函数之标量函数
 * @date 2021-05-19
 */
@Slf4j
public class ScalarFunctionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        // 编造测试数据
        List<Tuple3<Long, String, String>> ordersData = new ArrayList<>();
        ordersData.add(Tuple3.of(2L, "Euro", "A"));
        ordersData.add(Tuple3.of(1L, "US Dollar", "B"));
        ordersData.add(Tuple3.of(50L, "Yen", "C"));
        ordersData.add(Tuple3.of(3L, "Euro", "A"));

        DataStream<Tuple3<Long, String, String>> ordersDataStream = env.fromCollection(ordersData);
        ordersDataStream.print();


        //////////////////////////////////  Flink1.12 - 自定义函数的各种总结   /////////////////////////////
        /**
         *  Flink1.12 - 自定义函数的各种总结
         * 自定义函数（UDF）是一种扩展开发机制，可以用来在查询语句里调用难以用其他方式表达的频繁使用或自定义的逻辑。
         * 自定义函数可以用 JVM 语言（例如 Java 或 Scala）或 Python 实现，实现者可以在 UDF 中使用任意第三方库，本文聚焦于使用 JVM 语言开发自定义函数。
         * 当前 Flink 有如下几种函数：
         *    标量函数 将标量值转换成一个新标量值；
         *    表值函数 将标量值转换成新的行数据；
         *    聚合函数 将多行数据里的标量值转换成一个新标量值；
         *    表值聚合函数 将多行数据里的标量值转换成新的行数据；
         *    异步表值函数 是异步查询外部数据系统的特殊函数。
         *
         * 参考资料：
         * https://www.cnblogs.com/qiu-hua/p/14053566.html
         *
         */
        ////////////////////////////////////////////////////////////////////////////////////////////////





        log.info(" >>>>>>>>>>>>>>>>>>flink计算完毕... ");

        env.execute();

    }

    // 编造各种：最简单的自定义函数-ScalarFunction
    // 标量函数的入参可以是0个、1个或者多个值，然后返回值是一个值。实现标量函数需要继承抽象类ScalarFunction，然后定一个名称为eval且为public类型的函数。
    // 定义标量函数有多重方式
    /**
     * 接受两个int类型的参数，然后返回计算的sum值
     */
    public static class SumFunction extends ScalarFunction{
        public Integer eval(Integer a, Integer b){
            return a + b;
        }
    }

    /**
     * 接收非空的int或者boolean类型
     */
    public static class StringifyFunction extends ScalarFunction {
        public String eval(int i){
            return String.valueOf(i);
        }

        public String eval(boolean b){
            return String.valueOf(b);
        }
    }

//    /**
//     *接收非空的int或者boolean类型,通过注解方式实现，flink 1.11版本后支持
//     */
//    @FunctionHint(input=[@DataTypeHint("INT")])
//    @FunctionHint(input=[@DataTypeHint("BOOLEAN")])
//    public static  class StringifyFunction extends ScalarFunction {
//        public String eval(Object o) {
//            return o.toString();
//        }
//    }

    /**
     * 接收任何类型的值，然后把它们转成string
     */
    public static class StringifyFunction1 extends ScalarFunction{
        public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o){
            return o.toString();
        }
    }

}
