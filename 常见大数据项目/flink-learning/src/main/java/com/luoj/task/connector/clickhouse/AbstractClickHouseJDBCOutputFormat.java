package com.luoj.task.connector.clickhouse;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-07
 */
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractClickHouseJDBCOutputFormat<T> extends RichOutputFormat<T> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractClickHouseJDBCOutputFormat.class);


    private final String username;
    private final String password;
    private final String driverName = "ru.yandex.clickhouse.ClickHouseDriver";
    protected final String[] hosts;

    protected final List<Connection> connectionList;

    public AbstractClickHouseJDBCOutputFormat(String username, String password, String[] hosts) {
        this.username = username;
        this.password = password;
        this.hosts = hosts;
        this.connectionList = new ArrayList<>();
    }

    @Override
    public void configure(Configuration parameters) {
    }

    protected void establishConnection() throws SQLException, ClassNotFoundException {
        Class.forName(driverName);

        for (String host : this.hosts) {
            // jdbc:clickhouse://10.138.41.146:8123
            String url = "jdbc:clickhouse://" + host;
            Connection connection = DriverManager.getConnection(url, this.username, this.password);
            this.connectionList.add(connection);
        }
    }

    protected void closeDbConnection() throws IOException {
        for (Connection connection : this.connectionList) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException se) {
                    LOG.warn("JDBC connection could not be closed: " + se.getMessage());
                } finally {
                    connection = null;
                }
            }
        }


    }
}
