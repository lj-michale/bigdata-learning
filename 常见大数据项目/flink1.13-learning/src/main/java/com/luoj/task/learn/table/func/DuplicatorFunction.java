package com.luoj.task.learn.table.func;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-04
 */

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * 通过注册指定返回值类型，flink 1.11 版本开始支持
 */
@FunctionHint(output = @DataTypeHint("ROW< i INT, s STRING >"))
public class DuplicatorFunction extends TableFunction<Row> {
    public void eval(Integer i, String s) {
        collect(Row.of(i, s));
        collect(Row.of(i, s));
    }
}