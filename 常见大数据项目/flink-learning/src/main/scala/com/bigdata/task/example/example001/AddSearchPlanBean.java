package com.bigdata.task.example.example001;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

@Setter
@Getter
@ToString
public class AddSearchPlanBean {

    // 筛选方案名称
    private String name;

    // 用户ID搜索方案所属者
    private Long userId;

    // 适用业务场景,数据来源类型：100：总仓搜索方案；101：分仓搜索方案；102：出入库明细搜索方案
    private int sourceType;

    // 筛选方案详情
    private String plan;

    // 是否默认：1默认，0否
    private Byte defaultFlag;

    // 系统初始化方案：1是，0否
    private Byte systemFlag;


    public AddSearchPlanBean() {

    }

}
