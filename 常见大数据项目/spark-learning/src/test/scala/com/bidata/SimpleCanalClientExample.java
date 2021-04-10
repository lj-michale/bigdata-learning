//package com.bidata;
//
//import javax.mail.Message;
//import java.net.InetSocketAddress;
//
///**
// * @author lj.michale
// * @description  Canal客户端。
// * 注意：canal服务端只会连接一个客户端，当启用多个客户端时，其他客户端是就无法获取到数据。所以启动一个实例即可
// * @see <a href="https://github.com/alibaba/canal/wiki/ClientExample">官方文档：ClientSample代码</a>
// * @date 2021-04-11
// */
//public class SimpleCanalClientExample {
//
//    public static void main(String args[]) {
//
//        /**
//         * 创建链接
//         *      SocketAddress: 如果提交到canal服务端所在的服务器上运行这里可以改为 new InetSocketAddress(AddressUtils.getHostIp(), 11111)
//         *      destination 通服务端canal.properties中的canal.destinations = example配置对应
//         *      username：
//         *      password：
//         */
//        CanalConnector connector = CanalConnectors.newSingleConnector(
//                new InetSocketAddress("node1", 11111),
//                "example", "", "");
//        int batchSize = 1000;
//        int emptyCount = 0;
//        try {
//            connector.connect();
//            connector.subscribe(".*\\..*");
//            connector.rollback();
//            int totalEmptyCount = 120;
//            while (emptyCount < totalEmptyCount) {
//                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
//                long batchId = message.getId();
//                int size = message.getEntries().size();
//                if (batchId == -1 || size == 0) {
//                    emptyCount++;
//                    System.out.println("empty count : " + emptyCount);
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                    }
//                } else {
//                    emptyCount = 0;
//                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
//                    printEntry(message.getEntries());
//                }
//
//                connector.ack(batchId); // 提交确认
//                // connector.rollback(batchId); // 处理失败, 回滚数据
//            }
//
//            System.out.println("empty too many times, exit");
//        } finally {
//            connector.disconnect();
//        }
//    }
//
//    private static void printEntry(List<Entry> entrys) {
//        for (Entry entry : entrys) {
//            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
//                continue;
//            }
//
//            RowChange rowChage = null;
//            try {
//                rowChage = RowChange.parseFrom(entry.getStoreValue());
//            } catch (Exception e) {
//                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
//                        e);
//            }
//
//            EventType eventType = rowChage.getEventType();
//            System.out.println(String.format("================&gt; binlog[%s:%s] , name[%s,%s] , eventType : %s",
//                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
//                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
//                    eventType));
//
//            /**
//             * 如果只对某些库的数据操作，可以加如下判断：
//             * if("库名".equals(entry.getHeader().getSchemaName())){
//             *      //TODO option
//             *  }
//             *
//             * 如果只对某些表的数据变动操作，可以加如下判断：
//             * if("表名".equals(entry.getHeader().getTableName())){
//             *     //todo option
//             * }
//             *
//             */
//
//            for (RowData rowData : rowChage.getRowDatasList()) {
//                if (eventType == EventType.DELETE) {
//                    printColumn(rowData.getBeforeColumnsList());
//                } else if (eventType == EventType.INSERT) {
//                    printColumn(rowData.getAfterColumnsList());
//                } else {
//                    System.out.println("-------&gt; before");
//                    printColumn(rowData.getBeforeColumnsList());
//                    System.out.println("-------&gt; after");
//                    printColumn(rowData.getAfterColumnsList());
//                }
//            }
//        }
//    }
//
//    private static void printColumn(List<Column> columns) {
//        for (Column column : columns) {
//            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
//        }
//    }
//
//}
