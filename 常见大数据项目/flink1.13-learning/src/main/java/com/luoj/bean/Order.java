package com.luoj.bean;

import com.luoj.common.FieldDescrib;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-04
 */
@Slf4j
@Data
public class Order {

    /**订单商品集合(必填)*/
    @FieldDescrib(name = "订单商品集合(必填)", desc = "{\"orderId\":\"JG-374623-JSFAGFS\",\"goodsOrder\":22,\"goodsType\":2,\"goodsName\":\"西瓜\",\"piece\":5.6,\"remark\":\"\"}\n")
    private List<OrderItem> orderItemList;

    @FieldDescrib(name = "电子商务物流交易号（等同于订单号） (必填)", desc = "TR-51224972-HSJGDJGDH")
    private String orderNo;

    /** 付款方ID (必填)*/
    private Integer paySideId;

    /** 支付方式ID (必填)*/
    private Integer payModeId;

    /** 包装说明(必填) */
    private String packageDesc;

    /**订单种类**/
    private int order_type;

    /** 寄件联系人(必填) */
    private String sendLinkMan;

    /** 寄件手机 (必填)*/
    private String sendPhoneSms;

    /** 寄件地址 (必填)*/
    private String sendAddress;

    /** 收件联系人 (必填)*/
    private String dispatchLinkMan;

    /** 收件手机(必填)*/
    private String dispatchPhoneSms;

    /** 收件地址 (必填)*/
    private String dispatchAddress;

    /** 寄件省份 */
    private String sendProvince;

    /** 寄件城市 */
    private String sendCity;

    /** 寄件区县 */
    private String sendCounty;

    /** 收件省份 */
    private String dispatchProvince;

    /** 收件城市*/
    private String dispatchCity;

    /** 收件区县*/
    private String dispatchCounty;

    public Order(List<OrderItem> orderItemList, String orderNo, Integer paySideId, Integer payModeId, String packageDesc,
                 int order_type, String sendLinkMan, String sendPhoneSms, String sendAddress, String dispatchLinkMan,
                 String dispatchPhoneSms, String dispatchAddress, String sendProvince, String sendCity, String sendCounty,
                 String dispatchProvince, String dispatchCity, String dispatchCounty) {
        this.orderItemList = orderItemList;
        this.orderNo = orderNo;
        this.paySideId = paySideId;
        this.payModeId = payModeId;
        this.packageDesc = packageDesc;
        this.order_type = order_type;
        this.sendLinkMan = sendLinkMan;
        this.sendPhoneSms = sendPhoneSms;
        this.sendAddress = sendAddress;
        this.dispatchLinkMan = dispatchLinkMan;
        this.dispatchPhoneSms = dispatchPhoneSms;
        this.dispatchAddress = dispatchAddress;
        this.sendProvince = sendProvince;
        this.sendCity = sendCity;
        this.sendCounty = sendCounty;
        this.dispatchProvince = dispatchProvince;
        this.dispatchCity = dispatchCity;
        this.dispatchCounty = dispatchCounty;
    }

    public static class OrderGenerator implements DataGenerator<Order> {

        RandomDataGenerator generator;

        @Override
        public void open(String s, FunctionInitializationContext functionInitializationContext,
                         RuntimeContext runtimeContext) throws Exception {
            generator = new RandomDataGenerator();
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Order next() {
            return new Order(
                    null,
                    generator.nextHexString(20),
                    generator.nextInt(1,15),
                    generator.nextInt(1,5),
                    generator.nextHexString(20),
                    generator.nextInt(1,5),
                    generator.nextHexString(6),
                    generator.nextHexString(11),
                    generator.nextHexString(30),
                    generator.nextHexString(10),
                    generator.nextHexString(11),
                    generator.nextHexString(30),
                    generator.nextHexString(10),
                    generator.nextHexString(8),
                    generator.nextHexString(8),
                    generator.nextHexString(10),
                    generator.nextHexString(10),
                    generator.nextHexString(20)
            );
        }
    }
}
