package com.luoj.common;

import java.util.Random;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-02
 */
public class GenerateRandomDataUtils {

    public static Double getRandomPriceName() {
        Double[] types = {5.67, 2.35, 18.43, 345.32, 67.56, 97.21, 162.78, 1632.89};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }


    public static String getRandomUserName() {
        String[] types = {"张山","李四","王五","Jack","Role","Selme","Michale","Ken"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static String getRandomProductName() {
        String[] types = {"欢乐薯条","衣服","鞋子","J帽子","《安徒生童话全集》","蜂蜜","盆景","《SpringCloud微服务架构设计与开发》"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static String getRandomUserID() {
        String[] types = {"JG-3232242","JG-213517361","JG-97632672","JG-3424687624","JG-4357724934","JG-87652737","JG-43324827172","JG-9726242123"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static String getRandomProductID() {
        String[] types = {"Good-3232242","Good-213517361","Good-97632672","Good-3424687624","Good-4357724934","Good-87652737","Good-43324827172","Good-9726242123"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

}
