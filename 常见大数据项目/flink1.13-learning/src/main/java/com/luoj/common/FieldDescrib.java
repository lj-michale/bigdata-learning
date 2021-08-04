package com.luoj.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @descr 字段描述注解
 * @author orange
 * @date 2021/8/4 23:42
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface FieldDescrib {

    /** 字段名 */
    String name();

    /** 字段描述 */
    String desc() default "";
}

