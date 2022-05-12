package com.aurora.generator;

import lombok.Data;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;

@Data
public class OrderItem {

    private String orderId;

    /** 物品排序 */
    private Integer goodsOrder;

    /** 物品类型 */
    private Integer goodsType;

    /** 物品名称 */
    private String goodsName;

    /** 件数 */
    private Double piece;

    /** 备注 */
    private String remark;

    public OrderItem(String orderId, Integer goodsOrder, Integer goodsType, String goodsName, Double piece, String remark) {
        this.orderId = orderId;
        this.goodsOrder = goodsOrder;
        this.goodsType = goodsType;
        this.goodsName = goodsName;
        this.piece = piece;
        this.remark = remark;
    }

    public static class OrderItemGenerator implements DataGenerator<OrderItem> {

        RandomDataGenerator generator;

        @Override
        public void open(String s, FunctionInitializationContext functionInitializationContext, RuntimeContext runtimeContext) throws Exception {
            generator = new RandomDataGenerator();
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public OrderItem next() {
            return new OrderItem(
                    generator.nextHexString(10),
                    generator.nextInt(1, 10),
                    generator.nextInt(1, 10),
                    generator.nextHexString(20),
                    generator.nextT(20.0),
                    generator.nextHexString(20)
            );
        }
    }

}
