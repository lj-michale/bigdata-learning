package com.aurora.generate;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

/**
 * @descri 生成如下格式测试数据
 *         用户id|城市id|上行流量|下行流量|发生时间
 *         471|4|0.0745|0.3826|1609863101410
 *         495|0|0.5707|0.3274|1609863101410
 *
 * @author lj.michale
 * @date 2022-03-31
 */
@Slf4j
public class FakeTrafficRecordSource implements SourceFunction<String> {

    private static Random random = new Random();
    private volatile boolean isRunning = true;

    // sleep的毫秒数
    private long sleepMillis;

    public FakeTrafficRecordSource() {
        new FakeTrafficRecordSource(500L);
    }

    public FakeTrafficRecordSource(long sleepMills) {
        this.sleepMillis = sleepMills;
    }

    /**
     * @descri  run 发射元素
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(fakeTrafficRecordString());
            Thread.sleep(sleepMillis);
        }
    }

    /**
     * @descri  生成流量记录
     * 字段：用户id、城市id、上行流量、下行流量、发生时间
     */
    public String fakeTrafficRecordString() {
        int accountId = random.nextInt(500);
        int cityId = random.nextInt(5);
        double upTraffic = new BigDecimal(random.nextDouble()).setScale(4, RoundingMode.HALF_UP).doubleValue();
        double downTraffic = new BigDecimal(random.nextDouble()).setScale(4, RoundingMode.HALF_UP).doubleValue();
        long eventTime = System.currentTimeMillis();

        TrafficRecord trafficRecord = new TrafficRecord(accountId, cityId, upTraffic, downTraffic, eventTime);
        return trafficRecord.toString();
    }

    /**
     * @descri cancel 关闭source
     */
    @Override
    public void cancel() {
        isRunning = false;
    }

    class TrafficRecord {
        public int accountId;
        public int cityId;
        public double upTraffic;
        public double downTraffic;
        public long eventTime;

        public TrafficRecord() {
        }

        public TrafficRecord(int accountId, int cityId, double upTraffic, double downTraffic, long eventTime) {
            this.accountId = accountId;
            this.cityId = cityId;
            this.upTraffic = upTraffic;
            this.downTraffic = downTraffic;
            this.eventTime = eventTime;
        }

        @Override
        public String toString() {
            return
                    "" +
                            accountId
                            + "|" +
                            cityId
                            + "|" +
                            upTraffic
                            + "|" +
                            downTraffic
                            + "|" +
                            eventTime
                    ;
        }
    }

}
