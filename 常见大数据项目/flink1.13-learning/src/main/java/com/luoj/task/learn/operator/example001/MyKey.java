package com.luoj.task.learn.operator.example001;


import java.io.Serializable;
import java.util.Objects;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-20
 */
public class MyKey implements Serializable {

    private int value;

    public MyKey(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyKey myKey = (MyKey) o;
        return value == myKey.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}