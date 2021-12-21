package com.aurora.generate;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-22
 */
import com.aurora.bean.LngLat;
import com.aurora.common.LoadResourcesUtils;
import com.aurora.common.RandomUtil;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

class GenerateCustomerOrderSource implements SourceFunction<String> {

    private static final long serialVersionUID = 1L;

    private volatile boolean isRunning = true;


    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while(this.isRunning) {
            Thread.sleep(6000);
            String order = getDriverData();
            sourceContext.collect(order);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }

    //随机产生订单数据
    public String getDriverData() {
        Properties p = LoadResourcesUtils.getProperties("content.properties");

        String driverJson = p.getProperty("source.driverJson");

        String value = driverJson ;
        if(value.indexOf("%orderId") >= 0){
            value = value.replaceAll("%orderId", RandomUtil.getOrderId());
        }
        if(value.indexOf("%appId") >= 0){
            value = value.replaceAll("%appId",RandomUtil.getAppId());
        }
        if(value.indexOf("%serviceId") >= 0){
            value = value.replaceAll("%serviceId",RandomUtil.getServiceId());
        }
        if(value.indexOf("%passageId") >= 0){
            value = value.replaceAll("%passageId",RandomUtil.getPassageId());
        }
        if(value.indexOf("%driverId") >= 0){
            value = value.replaceAll("%driverId",RandomUtil.getDriverId());
        }
        if(value.indexOf("%startLoclatitude") >= 0){
            LngLat startLoc=RandomUtil.getCoordinate();
            value = value.replaceAll("%startLoclatitude",Double.toString(startLoc.latitude));
            value = value.replaceAll("%startLoclongitude",Double.toString(startLoc.longitude));
        }
        if(value.indexOf("%endLoclatitude") >= 0){
            LngLat endLoc=RandomUtil.getCoordinate();
            value = value.replaceAll("%endLoclatitude",Double.toString(endLoc.latitude));
            value = value.replaceAll("%endLoclongitude",Double.toString(endLoc.longitude));
        }
        if(value.indexOf("%loclatitude") >= 0){
            LngLat loc=RandomUtil.getCoordinate();
            value = value.replaceAll("%loclatitude",Double.toString(loc.latitude));
            value = value.replaceAll("%loclongitude",Double.toString(loc.longitude));
        }
        if(value.indexOf("%flag") >= 0){
            value = value.replaceAll("%flag",Integer.toString(RandomUtil.getFlag()));
        }
        if(value.indexOf("%pushFlag") >= 0){
            value = value.replaceAll("%pushFlag",Integer.toString(RandomUtil.getPushFlag()));
        }
        if(value.indexOf("%state") >= 0){
            value = value.replaceAll("%state",Integer.toString(RandomUtil.getState()));
        }
        if(value.indexOf("%d") >= 0){
            value = value.replaceAll("%d", RandomUtil.getNum().toString());
        }
        if(value.indexOf("%s") >= 0){
            value = value.replaceAll("%s", RandomUtil.getStr());
        }
        if(value.indexOf("%f") >= 0){
            value = value.replaceAll("%f",RandomUtil.getDoubleStr());
        }
        if(value.indexOf("%ts") >= 0){
            value = value.replaceAll("%ts",RandomUtil.getTimeStr());
        }
        if(value.indexOf("%tl") >= 0){
            value = value.replaceAll("%tl",RandomUtil.getTimeLongStr());
        }

        System.out.println(value);

        return value;
    }
}
