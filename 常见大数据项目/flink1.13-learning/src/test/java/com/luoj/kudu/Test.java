package com.luoj.kudu;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-08
 */
import java.sql.Timestamp;
import java.util.UUID;
import org.apache.kudu.client.*;

public class Test {

    private final static int OPERATION_BATCH = 500;

    //同时支持三个模式的测试用例
    public static void insertTestGeneric(KuduSession session, KuduTable table, SessionConfiguration.FlushMode mode,
                                         int recordCount) throws Exception {
        // SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND
        // SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC
        // SessionConfiguration.FlushMode.MANUAL_FLUSH
        session.setFlushMode(mode);
        if (SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC != mode) {
            session.setMutationBufferSpace(OPERATION_BATCH);
        }
        int uncommit = 0;

        for (int i = 0; i < recordCount; i++) {

            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            UUID uuid = UUID.randomUUID();
            row.addString("id", uuid.toString());
            row.addInt("int_value", 100);
            row.addLong("bigint_value", 10000L);

            Long gtmMillis;
            /* System.currentTimeMillis() 是从1970-01-01开始算的毫秒数(GMT), kudu API是采用纳秒数, 所以需要*1000
             另外, 考虑到我们是东8区时间, 所以转成Long型需要再加8个小时, 否则存到Kudu的时间是GTM, 比东8区晚8个小时
             */

            //方法1: 获取当前时间对应的GTM时区unix毫秒数
            gtmMillis=System.currentTimeMillis();

            //方法2: 将timestamp转成对应的GTM时区unix毫秒数
            Timestamp localTimestamp = new Timestamp(System.currentTimeMillis());
            gtmMillis=localTimestamp.getTime();

            //将GTM的毫秒数转成东8区的毫秒数量
            Long shanghaiTimezoneMillis=gtmMillis+8*3600*1000;
            row.addLong("timestamp_value", shanghaiTimezoneMillis*1000);

            session.apply(insert);

            // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
            if (SessionConfiguration.FlushMode.MANUAL_FLUSH == mode) {
                uncommit = uncommit + 1;
                if (uncommit > OPERATION_BATCH / 2) {
                    session.flush();
                    uncommit = 0;
                }
            }
        }

        // 对于手工提交, 保证完成最后的提交
        if (SessionConfiguration.FlushMode.MANUAL_FLUSH == mode && uncommit > 0) {
            session.flush();
        }

        // 对于后台自动提交, 必须保证完成最后的提交, 并保证有错误时能抛出异常
        if (SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND == mode) {
            session.flush();
            RowErrorsAndOverflowStatus error = session.getPendingErrors();
            if (error.isOverflowed() || error.getRowErrors().length > 0) {
                if (error.isOverflowed()) {
                    throw new Exception("Kudu overflow exception occurred.");
                }
                StringBuilder errorMessage = new StringBuilder();
                if (error.getRowErrors().length > 0) {
                    for (RowError errorObj : error.getRowErrors()) {
                        errorMessage.append(errorObj.toString());
                        errorMessage.append(";");
                    }
                }
                throw new Exception(errorMessage.toString());
            }
        }

    }

    //仅支持手动flush的测试用例
    public static void insertTestManual(KuduSession session, KuduTable table, int recordCount) throws Exception {
        // SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND
        // SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC
        // SessionConfiguration.FlushMode.MANUAL_FLUSH
        SessionConfiguration.FlushMode mode = SessionConfiguration.FlushMode.MANUAL_FLUSH;
        session.setFlushMode(mode);
        session.setMutationBufferSpace(OPERATION_BATCH);

        int uncommit = 0;

        for (int i = 0; i < recordCount; i++) {

            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            UUID uuid = UUID.randomUUID();
            row.addString("id", uuid.toString());
            row.addInt("int_value", 100);
            row.addLong("bigint_value", 10000L);


            Long gtmMillis;
            /* System.currentTimeMillis() 是从1970-01-01开始算的毫秒数(GMT), kudu API是采用纳秒数, 所以需要*1000
             另外, 考虑到我们是东8区时间, 所以转成Long型需要再加8个小时, 否则存到Kudu的时间是GTM, 比东8区晚8个小时
             */

            //方法1: 获取当前时间对应的GTM时区unix毫秒数
            gtmMillis=System.currentTimeMillis();


            //方法2: 将timestamp转成对应的GTM时区unix毫秒数
            Timestamp localTimestamp = new Timestamp(System.currentTimeMillis());
            gtmMillis=localTimestamp.getTime();

            //将GTM的毫秒数转成东8区的毫秒数量
            Long shanghaiTimezoneMillis=gtmMillis+8*3600*1000;
            row.addLong("timestamp_value", shanghaiTimezoneMillis*1000);

            session.apply(insert);

            // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
            uncommit = uncommit + 1;
            if (uncommit > OPERATION_BATCH / 2) {
                session.flush();
                uncommit = 0;
            }
        }

        // 对于手工提交, 保证完成最后的提交
        if (uncommit > 0) {
            session.flush();
        }
    }

    //仅支持自动flush的测试用例
    public static void insertTestInAutoSync(KuduSession session, KuduTable table, int recordCount) throws Exception {
        // SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND
        // SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC
        // SessionConfiguration.FlushMode.MANUAL_FLUSH
        SessionConfiguration.FlushMode mode = SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC;
        session.setFlushMode(mode);

        for (int i = 0; i < recordCount; i++) {

            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            UUID uuid = UUID.randomUUID();
            row.addString("id", uuid.toString());
            row.addInt("int_value", 100);
            row.addLong("bigint_value", 10000L);


            Long gtmMillis;
            /* System.currentTimeMillis() 是从1970-01-01开始算的毫秒数(GMT), kudu API是采用纳秒数, 所以需要*1000
             另外, 考虑到我们是东8区时间, 所以转成Long型需要再加8个小时, 否则存到Kudu的时间是GTM, 比东8区晚8个小时
             */

            //方法1: 获取当前时间对应的GTM时区unix毫秒数
            gtmMillis=System.currentTimeMillis();


            //方法2: 将timestamp转成对应的GTM时区unix毫秒数
            Timestamp localTimestamp = new Timestamp(System.currentTimeMillis());
            gtmMillis=localTimestamp.getTime();

            //将GTM的毫秒数转成东8区的毫秒数量
            Long shanghaiTimezoneMillis=gtmMillis+8*3600*1000;
            row.addLong("timestamp_value", shanghaiTimezoneMillis*1000);

            //对于AUTO_FLUSH_SYNC模式, apply()将立即完成kudu写入
            session.apply(insert);
        }
    }

    public static void test() throws KuduException {
        KuduClient client = new KuduClient.KuduClientBuilder("szgl-qa-pc-components08-992.jpushoa.com:7051,szgl-qa-pc-components07-998.jpushoa.com:7051,szgl-qa-pc-components09-993.jpushoa.com:7051,szgl-qa-pc-components08-992.jpushoa.com:7051,szgl-qa-pc-components04-996.jpushoa.com:7051,szgl-qa-pc-components07-998.jpushoa.com:7051,szgl-qa-pc-components06-989.jpushoa.com:7051,szgl-qa-pc-components05-990.jpushoa.com:7051,szgl-qa-pc-components09-993.jpushoa.com:7051")
                .build();
        KuduSession session = client.newSession();
        KuduTable table = client.openTable("testdb.tmp_test_perf");

        SessionConfiguration.FlushMode mode;
        Timestamp d1 = null;
        Timestamp d2 = null;
        long millis;
        long seconds;
        int recordCount = 0;

        try {

            mode = SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND;
            d1 = new Timestamp(System.currentTimeMillis());
            insertTestGeneric(session, table, mode, recordCount);
            d2 = new Timestamp(System.currentTimeMillis());
            millis = d2.getTime() - d1.getTime();
            seconds = millis / 1000 % 60;
            System.out.println(mode.name() + "耗时秒数:" + seconds);

            mode = SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC;
            d1 = new Timestamp(System.currentTimeMillis());
            insertTestInAutoSync(session, table,  recordCount);
            d2 = new Timestamp(System.currentTimeMillis());
            millis = d2.getTime() - d1.getTime();
            seconds = millis / 1000 % 60;
            System.out.println(mode.name() + "耗时秒数:" + seconds);

            mode = SessionConfiguration.FlushMode.MANUAL_FLUSH;
            d1 = new Timestamp(System.currentTimeMillis());
            insertTestManual(session, table,  recordCount);
            d2 = new Timestamp(System.currentTimeMillis());
            millis = d2.getTime() - d1.getTime();
            seconds = millis / 1000 % 60;
            System.out.println(mode.name() + "耗时秒数:" + seconds);


        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
        }

    }

    public static void main(String[] args) {
        try {
            test();
        } catch (KuduException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("Done");

    }
}
