package com.luoj.bean;


import lombok.Data;

/**
 * @author lj.michale
 * @description 学生的统计基础类
 * @date 2021-08-13
 */
@Data
public class StudentViewCount {

    private int id;

    /**
     * 窗口结束时间
     */
    private long windowEnd;

    /**
     * 同一个 id 下的统计数量
     */
    private long viewCount;

    public static StudentViewCount of(int id, long windowEnd, long count) {
        StudentViewCount result = new StudentViewCount();
        result.setId(id);
        result.setWindowEnd(windowEnd);
        result.setViewCount(count);
        return result;
    }
}