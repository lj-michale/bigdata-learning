package com.luoj.task.learn.table.func;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.codehaus.groovy.runtime.DefaultGroovyMethods.collect;

/**
 * @author lj.michale
 * @description  自定义函数之TableFunction
 * @date 2021-08-04
 */
public class SplitFunc {

    /**
     * @descr 单个eval方法
     */
    public static class Split extends TableFunction<Tuple2<String,Integer>> {
        private String separator = ",";

        public Split(String separator) {
            this.separator = separator;
        }

        public void eval(String str) {
            for (String s : str.split(separator)) {
                collect(new Tuple2<String,Integer>(s, s.length()));
            }
        }
    }

    /**
     * 注册多个eval方法，接收long或者string类型的参数，然后将他们转成string类型
     */
    public static class DuplicatorFunction extends TableFunction<String>{
        public void eval(Long i){
            eval(String.valueOf(i));
        }

        public void eval(String s){
            collect(s);
        }
    }

    /**
     * 接收不固定个数的int型参数,然后将所有数据依次返回
     */
    public static class FlattenFunction extends TableFunction<Integer>{
        public void eval(Integer... args){
            for (Integer i: args){
                collect(i);
            }
        }
    }


}
