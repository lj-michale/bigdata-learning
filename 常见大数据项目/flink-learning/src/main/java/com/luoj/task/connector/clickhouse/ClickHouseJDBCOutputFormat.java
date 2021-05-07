package com.luoj.task.connector.clickhouse;


import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * OutputFormat to write Rows into a JDBC database.
 * The OutputFormat has to be configured using the supplied OutputFormatBuilder.
 *
 * @see Row
 * @see DriverManager
 */
public class ClickHouseJDBCOutputFormat extends AbstractClickHouseJDBCOutputFormat<Row> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseJDBCOutputFormat.class);

    private final String query;

    private final List<PreparedStatement> preparedStatementList;

    private final Map<Integer, List<Row>> ipWithDataList;

    private final long insertCkTimenterval; // 4000L
    // 插入的批次
    private final int insertCkBatchSize;  // 开发测试用10条
    // 上次写入时间
    private Long lastInsertTime;

    private final String dataBaseName;
    private final String tablename;
    private final String[] tableColums;

    /**
     *
     * @param username              用户名
     * @param password              密码
     * @param hosts                 格式 {"1.1.1.1:8123", "1.1.1.2:8123", "1.1.1.3:8123"}
     * @param insertCkTimenterval   flush数据到 ClickHouse (ms)
     * @param insertCkBatchSize     达到多少条写 ClickHouse
     * @param dataBaseName          数据库名
     * @param tablename             表名 (本地表名)
     * @param tableColums           列名
     */
    public ClickHouseJDBCOutputFormat(String username, String password, String[] hosts, long insertCkTimenterval, int insertCkBatchSize, String dataBaseName, String tablename, String[] tableColums) {
        super(username, password, hosts);
        this.insertCkTimenterval = insertCkTimenterval;
        this.insertCkBatchSize = insertCkBatchSize;
        this.lastInsertTime = System.currentTimeMillis();
        this.ipWithDataList = new HashMap<>();
        this.dataBaseName = dataBaseName;
        this.tablename = tablename;
        this.tableColums = tableColums;
        this.preparedStatementList = new ArrayList<>();
        this.query = clickhouseInsertValue(this.tableColums, this.tablename, this.dataBaseName);
    }

    /**
     * Connects to the target database and initializes the prepared statement.
     *
     * @param taskNumber The number of the parallel instance.
     * @throws IOException Thrown, if the output could not be opened due to an
     *                     I/O problem.
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            establishConnection();

            for (Connection connection : connectionList) {
                PreparedStatement preparedStatement = connection.prepareStatement(query);
                this.preparedStatementList.add(preparedStatement);
            }

        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
        }
    }

    @Override
    public void writeRecord(Row row) throws IOException {
        /**
         * 1. 将数据写入CK
         */
        final int[] size = {0};
        ipWithDataList.values().forEach(rows -> {
            size[0] += rows.size();
        });

        if (size[0] >= this.insertCkBatchSize) {
            ipWithDataList.forEach((index, rows) -> {
                try {
                    flush(rows, preparedStatementList.get(index), connectionList.get(index));
                    LOG.info("insertCkBatchSize");
                } catch (SQLException e) {
                    throw new RuntimeException("Preparation of JDBC statement failed.", e);
                }
            });

            this.lastInsertTime = System.currentTimeMillis();

        }

        /**
         * 将当前行数据添加到List中
         */
        // 轮询写入各个local表，避免单节点数据过多
        if (null != row) {
            Random random = new Random();
            int index = random.nextInt(super.hosts.length);

            List<Row> rows = ipWithDataList.get(index);
            if (rows == null) {
                rows = new ArrayList<>();
            }

            rows.add(row);
            ipWithDataList.put(index, rows);
        }
    }


    // 插入数据
    public void flush(List<Row> rows, PreparedStatement preparedStatement, Connection connection) throws SQLException {

        for (int i = 0; i < rows.size(); ++i) {
            Row row = rows.get(i);
            for (int j = 0; j < this.tableColums.length; ++j) {

                if (null != row.getField(j)) {
                    preparedStatement.setObject(j + 1, row.getField(j));
                } else {
                    preparedStatement.setObject(j + 1, "null");
                }
            }
            preparedStatement.addBatch();
        }

        preparedStatement.executeBatch();
        connection.commit();
        preparedStatement.clearBatch();

        rows.clear();
    }

    public void snapshotStateFlush() {
        if (this.isTimeToDoInsert()) {

            LOG.info("timeToDoInsert");

            flush();
        }
    }

    public void flush() {
        ipWithDataList.forEach((index, rows) -> {
            try {
                flush(rows, preparedStatementList.get(index), connectionList.get(index));
            } catch (SQLException e) {
                throw new RuntimeException("Preparation of JDBC statement failed.", e);
            }
        });
    }

    /**
     * 根据时间判断是否插入数据
     *
     * @return
     */
    private boolean isTimeToDoInsert() {
        long currTime = System.currentTimeMillis();
        return currTime - this.lastInsertTime >= this.insertCkTimenterval;
    }

    /**
     * Executes prepared statement and closes all resources of this instance.
     *
     * @throws IOException Thrown, if the input could not be closed properly.
     */
    @Override
    public void close() throws IOException {
        for (PreparedStatement preparedStatement : this.preparedStatementList) {
            if (null != preparedStatement) {
                if (preparedStatement != null) {
                    flush();
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        LOG.info("JDBC statement could not be closed: " + e.getMessage());
                    } finally {
                        preparedStatement = null;
                    }
                }
            }
        }

        closeDbConnection();
    }

    private String clickhouseInsertValue(String[] tableColums, String tablename, String dataBaseName) {
        StringBuffer sbCloums = new StringBuffer();
        StringBuffer sbValues = new StringBuffer();
        for (String s : tableColums) {
            sbCloums.append(s).append(",");
            sbValues.append("?").append(",");
        }
        String colums = sbCloums.toString().substring(0, sbCloums.toString().length() - 1);
        String values = sbValues.toString().substring(0, sbValues.toString().length() - 1);

        String insertSQL = "INSERT INTO " + dataBaseName + "." + tablename + " ( " + colums + " ) VALUES ( " + values + ")";
        return insertSQL;
    }

}