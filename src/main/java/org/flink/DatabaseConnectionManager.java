package org.flink;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.util.Properties;

public class DatabaseConnectionManager {
    private static HikariDataSource db1DataSource;
    private static HikariDataSource db2DataSource;

    // Initialize the DataSources
    public static synchronized void initialize(Properties props) {
        System.out.println(props);
        if (db1DataSource == null) {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(props.getProperty("DB1_URL"));
            config.setUsername(props.getProperty("DB1_USERNAME"));
            config.setPassword(props.getProperty("DB1_PASSWORD"));
            config.setDriverClassName("org.postgresql.Driver");
            config.setMaximumPoolSize(Integer.parseInt(props.getProperty("DB1_MAX_POOL_SIZE", "10")));
            config.setIdleTimeout(Long.parseLong(props.getProperty("DB1_IDLE_TIMEOUT_MS", "60000")));
            config.setMaxLifetime(Long.parseLong(props.getProperty("DB1_MAX_LIFETIME_MS", "1800000")));
            config.setConnectionTimeout(Long.parseLong(props.getProperty("DB1_CONNECTION_TIMEOUT_MS", "30000")));
            config.setConnectionTestQuery("SELECT 1");
            db1DataSource = new HikariDataSource(config);
        }

        if (db2DataSource == null) {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(props.getProperty("DB2_URL"));
            config.setUsername(props.getProperty("DB2_USERNAME"));
            config.setPassword(props.getProperty("DB2_PASSWORD"));
            config.setDriverClassName("com.mysql.cj.jdbc.Driver");
            config.setMaximumPoolSize(Integer.parseInt(props.getProperty("DB2_MAX_POOL_SIZE", "10")));
            config.setIdleTimeout(Long.parseLong(props.getProperty("DB2_IDLE_TIMEOUT_MS", "60000")));
            config.setMaxLifetime(Long.parseLong(props.getProperty("DB2_MAX_LIFETIME_MS", "1800000")));
            config.setConnectionTimeout(Long.parseLong(props.getProperty("DB2_CONNECTION_TIMEOUT_MS", "30000")));
            config.setConnectionTestQuery("SELECT 1");
            db2DataSource = new HikariDataSource(config);
        }
    }

    // Get a connection for DB1 (PostgreSQL)
    public static Connection getDB1Connection() throws Exception {
        if (db1DataSource == null) {
            throw new IllegalStateException("DB1 DataSource not initialized. Call initialize() first.");
        }
        return db1DataSource.getConnection();
    }

    // Get a connection for DB2 (MySQL)
    public static Connection getDB2Connection() throws Exception {
        if (db2DataSource == null) {
            throw new IllegalStateException("DB2 DataSource not initialized. Call initialize() first.");
        }
        return db2DataSource.getConnection();
    }

    // Close both DataSources
    public static synchronized void close() {
        if (db1DataSource != null) {
            db1DataSource.close();
        }
        if (db2DataSource != null) {
            db2DataSource.close();
        }
    }
}
