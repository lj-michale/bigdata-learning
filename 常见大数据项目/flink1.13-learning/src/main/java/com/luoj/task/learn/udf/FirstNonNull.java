package com.luoj.task.learn.udf;

import org.apache.flink.table.functions.AggregateFunction;
import java.util.ArrayList;

/**
 * @author lj.michale
 * @description
 *   一个aggFunction必须要实现的方法有：
 *   createAccumulator创建accumulator
 *   accumulate(ACC accumulator, [user defined inputs])
 *   getValue返回结果
 *
 * 1.UDF: 自定义标量函数(User Defined Scalar Function)。一行输入一行输出。
 * 2.UDAF: 自定义聚合函数。多行输入一行输出。
 * 3.UDTF: 自定义表函数。一行输入多行输出或一列输入多列输出
 * @date 2021-05-27
 */
public class FirstNonNull extends AggregateFunction<String[],ArrayList<String>> {

    @Override
    public ArrayList<String> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public String[] getValue(ArrayList<String> data) {
        if (data == null || data.size() == 0) {
            return null;
        }
        return data.toArray(new String[data.size()]);
    }

    public void accumulate(ArrayList<String> src, String... input) {
        if (src.size() == 0) {
            addAll(src, input);
        } else {
            String curr_order_by_value = String.valueOf(input[0]);
            String src_order_by_value = String.valueOf(src.get(0));
            if (src_order_by_value.compareTo(curr_order_by_value) > 0) {
                addAll(src, input);
            } else if (src.contains(null)) {
                fillNull(src, input);
            }
        }
    }

    public void fillNull(ArrayList<String> src, String[] input) {
        int size = src.size();
        for (int i = 0; i < size; i++) {
            if (src.get(i) == null) {
                src.set(i, input[i] == null ? null : String.valueOf(input[i]));
            }
        }
    }

    public void addAll(ArrayList<String> src, String[] input) {
        for (int i = 0; i < input.length; i++) {
            Object value = input[i];
            if (i >= src.size()) {
                src.add(i, value == null ? null : String.valueOf(value));
            } else {
                if (value != null) {
                    src.set(i, String.valueOf(value));
                }
            }
        }
    }

}
