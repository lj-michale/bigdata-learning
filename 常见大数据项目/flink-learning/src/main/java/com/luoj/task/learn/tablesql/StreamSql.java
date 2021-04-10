package com.luoj.task.learn.tablesql;

/**
 * @author lj.michale
 * @description
 * @date 2021-04-10
 */
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
//flink1.10时候是import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.*;
import java.sql.Timestamp;

/**
 * 目前flink1.11,回不去flink1.10
 */
public class StreamSql {

    public static void main(String[] args) throws Exception {

        /**
         * 1 注册环境
         */
        EnvironmentSettings mySetting = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, mySetting);

        /**
         * 2 数据源
         */
        DataStream<String> inputsteam = env .readTextFile("D:\\Workplace\\IdeaProjects\\other_project\\flink-learning\\TableApiAndSql\\src\\main\\resources\\datafile\\temperature.txt");
        // inputsteam.print();

        DataStream<Sensor> dataStream = inputsteam.map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new Sensor(
//                        String.valueOf(split[0]),
//                        Timestamp.valueOf(split[1]),
//                        Double.valueOf(split[2]),
//                        Double.valueOf(split[3])
                );
            }
        });
        //@deprecated 不推荐 tableEnv.registerTable("TheDataTable",dataTable);。旧版用法现在是创建表或者view
        tableEnv.createTemporaryView("TheDataTable",dataStream);


        /**
         * 4 转换回数据流，打印输出.
         */

        /**
         * 4，1 flink_sql_jdbc输出
         */
        //flink1.10中'connector.driver' 可以写也可以不写。optional: the class name of the JDBC driver to use to connect to this URL. If not set, it will automatically be derived from the URL.
        //create中VARCHAR(10)等长度一直不规范，DOUBLE()肯定报错,DECIMAL(5,2)也报错.总结数据库用DECIMAL，java用double就可以
        //类型需要内部保持一致，而不是和数据库类型保持一致。如数据库是TIMESTAMP(6)，但这个类处理内部是TIMESTAMP(3)
        String sinkFlink1_10_DDL =
                "CREATE TABLE jdbcOutputTable (                                       "
                        + "       tab STRING                                                    "
//                + "       tab VARCHAR                                                   "
                        + "      ,record_time TIMESTAMP(3)                                      "
                        + "      ,temperature DOUBLE                                            "
                        + "      ,humidity DOUBLE                                               "
//                + "       ,  humidity DECIMAL(5,2)                                             "
                        + ") WITH (                                                             "
                        + "        'connector.type' = 'jdbc',                                   "
                        + "        'connector.url' = 'jdbc:mysql://192.168.109.139:3306/kgxdb', "
                        + "        'connector.table' = 'sensor',                                "
                        + "        'connector.username' = 'root',                               "
                        + "        'connector.password' = 'qwertyuiop1234567890',               "
                        + "        'connector.write.flush.max-rows' = '1'                       "
                        + ")";

        //flink1.11的新版create
        String sinkFlink1_11_DDL =
                "CREATE TABLE jdbcOutputTable (                              \n" +
                        "    tab STRING,                                             \n" +
                        "    record_time TIMESTAMP(3),                               \n" +
                        "    temperature DOUBLE,                                     \n" +
                        "    humidity DOUBLE,                                        \n" +
                        "    PRIMARY KEY (tab) NOT ENFORCED                            " +
                        ") WITH (                                                    \n" +
                        "   'connector'  = 'jdbc',                                   \n" +
                        "   'url'        = 'jdbc:mysql://192.168.109.139:3306/kgxdb',\n" +
                        "   'table-name' = 'sensor',                                 \n" +
                        "   'driver'     = 'com.mysql.jdbc.Driver',                  \n" +
                        "   'username'   = 'root',                                   \n" +
                        "   'password'   = 'qwertyuiop1234567890'                    \n" +
                        ")";
        tableEnv.executeSql(sinkFlink1_11_DDL);

        /**
         * 3.1 TableSql
         */
//        Table ResultSql_Table = tableEnv.sqlQuery("select tab,record_time,temperature,humidity from TheDataTable where tab in ('sensor_1','sensor_2')");
//        String explain = tableEnv.explain(ResultSql_Table); System.out.println("执行计划为\n"+explain);
//        ResultSql_Table.insertInto("jdbcOutputTable");

        /**
         * 3.2 优化3.1
         */
        //flink1.10的版本
//        tableEnv.executeSql( "INSERT INTO jdbcOutputTable " +
//                "SELECT tab,record_time,temperature,humidity FROM TheDataTable WHERE tab IN ('sensor_1','sensor_2','sensor_3')");
        //flink1.11的TableResult
        TableResult tableResult1 = tableEnv.executeSql( "INSERT INTO jdbcOutputTable " +
                "SELECT tab,record_time,temperature,humidity FROM TheDataTable WHERE tab IN ('sensor_1','sensor_2','sensor_3')");
        System.out.println(tableResult1.getJobClient().get().getJobStatus());

        /**
         * 4.2 flink sql_jdbc输入。目前flink1.10的DDL不支持primary key。也没必要。
         */
        String sinkFlink1_10_DDL1 =
                "CREATE TABLE jdbcOutputTable1 (                                                "
                        + "      id DOUBLE                                                      "
                        + "      ,name STRING                                                   "
                        + "      ,total BIGINT                                                  "
//                        + "      ,primary key (id)                                              "
                        + ") WITH (                                                             "
                        + "        'connector.type' = 'jdbc',                                   "
                        + "        'connector.url' = 'jdbc:mysql://192.168.109.139:3306/kgxdb', "
                        + "        'connector.table' = 'report',                                "
                        + "        'connector.username' = 'root',                               "
                        + "        'connector.password' = 'qwertyuiop1234567890'                "
                        + ")";

        //flink1.11的新版create
        String sinkFlink1_11_DDL1 =
                "CREATE TABLE jdbcOutputTable1 (                                     \n" +
                        "    id DOUBLE,                                              \n" +
                        "    name STRING ,                                           \n" +
                        "    total BIGINT,                                           \n" +
                        "    PRIMARY KEY (id) NOT ENFORCED                            " +
                        ") WITH (                                                    \n" +
                        "   'connector'  = 'jdbc',                                   \n" +
                        "   'url'        = 'jdbc:mysql://192.168.109.139:3306/kgxdb',\n" +
                        "   'table-name' = 'report',                                 \n" +
                        "   'driver'     = 'com.mysql.jdbc.Driver',                  \n" +
                        "   'username'   = 'root',                                   \n" +
                        "   'password'   = 'qwertyuiop1234567890'                    \n" +
                        ")";
        tableEnv.executeSql(sinkFlink1_11_DDL1);

        //flink1.10时代：sqlQuery()不支持"INSERT INTO table—a SELECT * from table—a
        //只能两层select。否则会报 ”UpsertStreamTableSink requires that Table has a full primary keys if it is updated.“
        Table ResultSql_Table1 = tableEnv.sqlQuery(
                "SELECT id,name,total FROM (" +
                        "SELECT floor( rand()*100 ) AS id,tab AS name,COUNT(1) AS total " +
                        "FROM jdbcOutputTable where tab in ('sensor_1','sensor_2') group by tab )");

        ResultSql_Table1.printSchema();
        String explain1 = tableEnv.explain(ResultSql_Table1);
        System.out.println("执行计划为"+explain1);

        ResultSql_Table1.executeInsert("jdbcOutputTable1");

        //flink1.10。不能像下面这么玩。会报”SQL parse failed. Encountered "jdbcOutputTable" at line 1, column 123.“
       /* tableEnv.sqlUpdate("insert into jdbcOutputTable1 " +
                "SELECT id,name,total FROM (" +
                "SELECT floor( rand()*100 ) AS id,tab AS name,COUNT(1) AS total " +
                "FROM jdbcOutputTable where tab in ('sensor_1','sensor_2' ) group by tab ) as t");*/

        /**
         * 6.
         */
        env.execute("Start Table sql for Stream");


    }
}