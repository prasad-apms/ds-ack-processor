package org.flink;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.util.Properties;

public class DatabaseConnectionManager {
    private static HikariDataSource dataSource;

    // Initialize the DataSource
    public static synchronized void initialize(Properties props) {
        if (dataSource == null) {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(props.getProperty("DB1_URL"));
            config.setUsername(props.getProperty("DB1_USERNAME"));
            config.setPassword(props.getProperty("DB1_PASSWORD"));
            config.setDriverClassName("org.postgresql.Driver");
            config.setMaximumPoolSize(Integer.parseInt(props.getProperty("MAX_POOL_SIZE", "10")));
            config.setIdleTimeout(Long.parseLong(props.getProperty("IDLE_TIMEOUT_MS", "60000")));
            config.setMaxLifetime(Long.parseLong(props.getProperty("MAX_LIFETIME_MS", "1800000")));
            config.setConnectionTimeout(Long.parseLong(props.getProperty("CONNECTION_TIMEOUT_MS", "30000")));
            config.setConnectionTestQuery("SELECT 1");

            dataSource = new HikariDataSource(config);
        }
    }

    // Get a database connection
    public static Connection getConnection() throws Exception {
        if (dataSource == null) {
            throw new IllegalStateException("DatabaseConnectionManager not initialized. Call initialize() first.");
        }
        return dataSource.getConnection();
    }

    // Close the DataSource
    public static synchronized void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
