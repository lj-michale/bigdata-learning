package com.luoj.task.example.example001;

import lombok.*;
import java.io.Serializable;

/**
 * 订单消息体:
 * {"userId":234567,"orderId":2902306918400,"subOrderId":2902306918401,"siteId":10219,"siteName":"site_blabla","cityId":101,"cityName":"北京市","warehouseId":636,"merchandiseId":187699,"price":299,"quantity":2,"orderStatus":1,"isNewOrder":0,"timestamp":1572963672217}
 *
 * */

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class SubOrderDetail implements Serializable {

    private static final long serialVersionUID = 1L;

    private long userId;

    private long orderId;

    private long subOrderId;

    private long siteId;

    private String siteName;

    private long cityId;

    private String cityName;

    private long warehouseId;

    private long merchandiseId;

    private long price;

    private long quantity;

    private int orderStatus;

    private int isNewOrder;

    private long timestamp;

}