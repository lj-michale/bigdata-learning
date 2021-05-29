package com.luoj.task.learn.operator.example003.v2;

import java.util.Objects;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-29
 */
public class MyValue {
    private int value;

    public MyValue() {}

    public MyValue(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyValue myValue = (MyValue) o;
        return value == myValue.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}