package com.aurora.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * @author lj.michale
 * @description  自定义Sourcce,随机生成订单信息数据
 * @date 2021-08-19
 */
@Slf4j
public class GenerateCustomOrderSource implements SourceFunction<Tuple7<String, String, String, String, String, Double, String>>{

    private static final long serialVersionUID = 1L;
    private volatile boolean isRunning = true;
    private int count = 0;
    /**订单编号前缀*/
    public static final String PREFIX = "O-";

    /**
     * @descr 重写run方法实现自定义数据生成
     * @param ctx
     */
    @Override
    public void run(SourceContext<Tuple7<String, String, String, String, String, Double, String>> ctx) throws Exception {
        while (isRunning && count < 10000000) {
            String orderId = getOrderId(5);
            String customerId = getCustomerId();
            String productId = getProductId();
            String productName = getProductName();
            String price = getPrice(productName);
            Double buyMoney = getBuyMoney(Double.valueOf(price.toString()));
            String buyTime = getBuyTime();
            ctx.collect(new Tuple7<>(orderId, customerId, productId, productName, price, buyMoney, buyTime));
            count ++;
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    /****************************   随机生成OrderID  *********************************/
    /**
     * @descr 生成订单编号(aurora+yyyyMMddHHmmss+digit位随机数)
     * @param digit 生成几位随机数
     * @return 订单编号(String)
     */
    private static synchronized String getOrderId(int digit) {
        StringBuffer resultBuffer = new StringBuffer();
        resultBuffer.append(PREFIX).append(new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));
        resultBuffer.append("-");
        Random random = new Random();
        for (int i = 0; i < digit; i++) {
            resultBuffer.append(random.nextInt(10));
        }
        return resultBuffer.toString();
    }

    private String getCustomerId() {
        String[] customerArr = {"C-00001","C-00002","C-00003","C-00004","C-00005","C-00006","C-00007"};
        int index = (int) (Math.random() * customerArr.length);
        String rand = customerArr[index];
        return rand;
    }

    private String getProductId() {
        String[] productIdArr = {"P-00001","P-00002","P-00003","P-00004","P-00005","P-00006","P-00007"};
        int index = (int) (Math.random() * productIdArr.length);
        String rand = productIdArr[index];
        return rand;
    }

    private String getProductName() {
        String[] ProductNameArr = {"烤肉","椰子鸡","麻辣火锅","狮子头","清蒸婉鱼","麻婆豆腐","泰国香米"};
        int index = (int) (Math.random() * ProductNameArr.length);
        String rand = ProductNameArr[index];
        return rand;
    }

    private String getPrice(String productName) {
        String[] priceArr = {"2.56","3.78","16.98","98.35","176.34","63.25","7.89"};
        String price = "";
        switch(productName) {
            case "烤肉" : price = priceArr[0]; break;
            case "椰子鸡" : price = priceArr[1]; break;
            case "麻辣火锅" : price = priceArr[2]; break;
            case "狮子头" : price = priceArr[3]; break;
            case "清蒸婉鱼" : price = priceArr[4]; break;
            case "麻婆豆腐" : price = priceArr[5]; break;
            case "泰国香米" : price = priceArr[6]; break;
            default: System.out.println("没有找到该商品价格");
        }
        return price;
    }

    private Double getBuyMoney(Double price) {
        Random random = new Random();
        return getRound(price * random.nextInt(20),2);
    }

    private String getBuyTime() {
        long currentTimeMillis = System.currentTimeMillis();
        Date date = new Date(currentTimeMillis);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(date);
    }

    /**
     * @descr Double四舍五入
     * @param v
     * @param rr
     */
    public double getRound(double v, int rr){
        BigDecimal b=new BigDecimal(Double.toString(v));
        BigDecimal one=new BigDecimal(1);
        return b.divide(one,rr, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

}
