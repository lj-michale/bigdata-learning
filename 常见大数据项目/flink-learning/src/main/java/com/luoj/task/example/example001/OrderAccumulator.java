package com.luoj.task.example.example001;

import lombok.*;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class OrderAccumulator {

    private Long siteId;

    private String siteName;

    public void addOrderId(Long OrderId) {

    }

    public void addSubOrderSum(Integer integer) {

    }

    public void addQuantitySum(Long quantity) {

    }

    public void addGmv(Long gmv) {

    }

    public void addOrderIds(List<Long> addOrderIds) {

    }

    public List<Long> getOrderIds() {
        List<Long> longList = new ArrayList<>();
        return longList;

    }

    public Integer getSubOrderSum (){
        return 0;
    }

    public Long getQuantitySum(){
        return 0L;
    }

    public Long getGmv(){
        return 0L;
    }

}
