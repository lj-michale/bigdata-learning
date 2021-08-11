package com.luoj.task.learn.func;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;

import java.util.Optional;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-11
 */
public class LiteralFunction2 {

    public static class LiteralFunction extends ScalarFunction {
        public Object eval(String s, String type) {
            switch (type) {
                case "INT":
                    return Integer.valueOf(s);
                case "DOUBLE":
                    return Double.valueOf(s);
                case "STRING":
                default:
                    return s;
            }
        }

        // 禁用自动的反射式类型推导，使用如下逻辑进行类型推导
        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    // 指定输入参数的类型，必要时参数会被隐式转换
                    .typedArguments(DataTypes.STRING(), DataTypes.STRING())
                    // specify a strategy for the result data type of the function
                    .outputTypeStrategy(callContext -> {
                        if (!callContext.isArgumentLiteral(1) || callContext.isArgumentNull(1)) {
                            throw callContext.newValidationError("Literal expected for second argument.");
                        }
                        // 基于字符串值返回数据类型
                        final String literal = callContext.getArgumentValue(1, String.class).orElse("STRING");
                        switch (literal) {
                            case "INT":
                                return Optional.of(DataTypes.INT().notNull());
                            case "DOUBLE":
                                return Optional.of(DataTypes.DOUBLE().notNull());
                            case "STRING":
                            default:
                                return Optional.of(DataTypes.STRING());
                        }
                    })
                    .build();
        }
    }

}
