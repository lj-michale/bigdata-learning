//package com.luoj.task.learn.hbase;
//
//import com.luoj.common.MySQLGlobalConfig;
//import com.luoj.common.MySQLJDBCUtil;
//import org.apache.flink.api.common.functions.RichMapFunction;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
////import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
////import org.apache.flink.api.java.io.jdbc.split.NumericBetweenParametersProvider;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.configuration.ConfigConstants;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.types.Row;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Mutation;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//
//import java.sql.Connection;
//import java.sql.ResultSet;
//import java.sql.Statement;
//
///**
// * @author lj.michale
// * @description
// * @date 2021-07-27
// */
//public class fullPullApp {
//    public static final boolean isparallelism = true;
//
//    //分割字段
//    public static final String SPLIT_FIELD = "goodsId";
//
//    public static final RowTypeInfo ROW_TYPE_INFO = new RowTypeInfo(
//            BasicTypeInfo.INT_TYPE_INFO,
//            BasicTypeInfo.STRING_TYPE_INFO,
//            BasicTypeInfo.BIG_DEC_TYPE_INFO,
//            BasicTypeInfo.INT_TYPE_INFO,
//            BasicTypeInfo.INT_TYPE_INFO
//    );
//
//    public static void main(String[] args) throws Exception {
//
//        //获取实现环境
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
//        //读取商品表
//        JDBCInputFormat.JDBCInputFormatBuilder jdbcInputFormatBuilder = JDBCInputFormat.buildJDBCInputFormat()
//                .setDrivername(MySQLGlobalConfig.MySQL_DRIVER_CLASS)
//                .setDBUrl(MySQLGlobalConfig.MySQL_URL)
//                .setUsername(MySQLGlobalConfig.MySQL_NAME)
//                .setPassword(MySQLGlobalConfig.MySQL_PASSWORD)
//                .setQuery("select * from zyd_goods")
//                .setRowTypeInfo(ROW_TYPE_INFO);
//
//        if (isparallelism) {
//            int fetchSize = 2;
//            Boundary boundary = boundaryQuery(SPLIT_FIELD);
//            jdbcInputFormatBuilder.setQuery("select * from zyd_goods where "+SPLIT_FIELD+" between ? and ?")
//                    .setParametersProvider(new NumericBetweenParametersProvider(fetchSize,boundary.min,boundary.max));
//        }
//        //读取MySQL数据
//        DataSet<Row> source = env.createInput(jdbcInputFormatBuilder.finish());
//        source.print();
//        //生成hbase的输出数据
//        DataSet<Tuple2<Text, Mutation>> hbaseResult = convertMysqlToHbase(source);
//        //数据输出到hbase
//        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "note01,note02,note03");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("zookeeper.znode.parent", "/hbase");
//        conf.set(TableOutputFormat.OUTPUT_TABLE, "learing_flink:zyd_goods");
//        conf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp");
//        //新建一个job实例
//        Job job = Job.getInstance(conf);
//        hbaseResult.output(new HadoopOutputFormat<Text, Mutation>(new TableOutputFormat<>(), job));
//
//        env.execute("FullPullerAPP");
//    }
//
//    private static DataSet<Tuple2<Text, Mutation>> convertMysqlToHbase(DataSet<Row> dataSet) {
//        return dataSet.map(new RichMapFunction<Row, Tuple2<Text, Mutation>>() {
//            private transient Tuple2<Text, Mutation> resultTp;
//            private byte[] cf = "F".getBytes(ConfigConstants.DEFAULT_CHARSET);
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//                resultTp = new Tuple2<>();
//            }
//
//            @Override
//            public Tuple2<Text, Mutation> map(Row value) throws Exception {
//                resultTp.f0 = new Text(value.getField(0).toString());
//                Put put = new Put(value.getField(0).toString().getBytes(ConfigConstants.DEFAULT_CHARSET));
//                if (null != value.getField(1)) {
//                    put.addColumn(cf, Bytes.toBytes("goodsName"), Bytes.toBytes(value.getField(1).toString()));
//                }
//
//                put.addColumn(cf, Bytes.toBytes("sellingPrice"), Bytes.toBytes(value.getField(2).toString()));
//
//                put.addColumn(cf, Bytes.toBytes("goodsStock"), Bytes.toBytes(value.getField(3).toString()));
//
//                put.addColumn(cf, Bytes.toBytes("appraiseNum"), Bytes.toBytes(value.getField(4).toString()));
//
//                resultTp.f1 = put;
//                return resultTp;
//            }
//        });
//    }
//
//    // 预先查询数据库中最大值与最小值
//    public static Boundary boundaryQuery(String splitField) throws Exception {
//        String sql = "select min(" + splitField + ") , max(" + splitField + ") from zyd_goods";
//        Connection connection = null;
//        Statement statement = null;
//        ResultSet resultSet = null;
//        int min = 0;
//        int max = 0;
//
//        try {
//
//            connection = MySQLJDBCUtil.getConnection();
//            statement = connection.createStatement();
//            resultSet = statement.executeQuery(sql);
//
//            while (resultSet.next()) {
//                min = resultSet.getInt(1);
//                max = resultSet.getInt(2);
//
//                System.out.println(min + "------------------" + max);
//            }
//
//        } finally {
//            MySQLJDBCUtil.close(resultSet, statement, connection);
//        }
//
//        return Boundary.of(min, max);
//
//    }
//
//    public static class Boundary {
//        private int min;
//        private int max;
//
//        public Boundary(int min, int max) {
//            this.min = min;
//            this.max = max;
//
//        }
//
//        public static Boundary of(int min, int max) {
//            return new Boundary(min, max);
//        }
//    }
//
//}