package org.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.flink.config.DatabaseConnectionManager;
import org.flink.models.Message;
import org.flink.utils.Helper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;
public class SyncAckToDb extends RichSinkFunction<Message> {

    private static final Logger LOG = LoggerFactory.getLogger(SyncAckToDb.class);

    private static final String LOG_ACK_UPDATE_QUERY = "UPDATE dw.log_mc_ed_sync SET status = 1, duration = ?::interval WHERE sync_id = ?";
    private static final String IS_LOCK_UPDATE_QUERY = "UPDATE mc_machines SET is_lock = ? WHERE machine_id = ?";
    private static final String FETCH_REQ_LOG_DATA_QUERY = "SELECT trigger_type AS reqType, trigger_ref AS tRef, machine_id AS mcId, cdt AS startTime, udt AS endTime " +
            "FROM dw.log_mc_ed_sync WHERE sync_id = ? LIMIT 1";

    private transient Connection db1Connection;
    private transient PreparedStatement db1UpdateStatement;

    private transient Connection db2Connection;
    private transient PreparedStatement db2UpdateStatement;

    private final Properties properties;

    public SyncAckToDb(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Load database drivers
        Class.forName("org.postgresql.Driver");
        Class.forName("com.mysql.cj.jdbc.Driver");

        // Initialize connections using a connection manager
        DatabaseConnectionManager.initialize(properties);

        // Get the shared connection.
        db1Connection = DatabaseConnectionManager.getDB1Connection();
        db2Connection = DatabaseConnectionManager.getDB2Connection();

        // Prepare statements
        db1UpdateStatement = db1Connection.prepareStatement(LOG_ACK_UPDATE_QUERY);
        db2UpdateStatement = db2Connection.prepareStatement(IS_LOCK_UPDATE_QUERY);

        // Disable auto-commit for transactional safety
        db1Connection.setAutoCommit(false);
        db2Connection.setAutoCommit(false);
    }



    @Override
    public void invoke(Message event, Context context) {
        if (event == null) {
            LOG.warn("Received null event, skipping processing.");
            return;
        }

        try {
            processEvent(event);
        } catch (SQLException e) {
            LOG.error("Error processing event with ID {}: {}", event.getReqId(), e.getMessage(), e);
        } catch (Exception e) {
            LOG.error("Unexpected error while processing event: {}", e.getMessage(), e);
        }
    }



    private void processEvent(Message event) throws SQLException {
        try (PreparedStatement fetchStatement = db1Connection.prepareStatement(FETCH_REQ_LOG_DATA_QUERY)) {
            fetchStatement.setString(1, event.getReqId());

            try (ResultSet resultSet = fetchStatement.executeQuery()) {
                if (resultSet.next()) {
                    handleFetchedData(resultSet, event);
                } else {
                    LOG.warn("No data found for reqId: {}", event.getReqId());
                }
            }
        }
    }


    private void handleFetchedData(ResultSet resultSet, Message event) throws SQLException {
        int reqType = resultSet.getInt("reqType");
        int triggerReference = resultSet.getInt("tRef");
        int mcId = resultSet.getInt("mcId");
        Timestamp startTime = resultSet.getTimestamp("startTime");
        Timestamp endTime = resultSet.getTimestamp("endTime");

        String duration = Helper.convertToSessionDuration(startTime, endTime);

        // Update log status and duration
        db1UpdateStatement.setString(1, duration);
        db1UpdateStatement.setString(2, event.getReqId());

        try {
            db1UpdateStatement.executeUpdate();
            db1Connection.commit();
            LOG.info("Updated log status and duration for reqId: {}", event.getReqId());
        } catch (SQLException e) {
            db1Connection.rollback();
            LOG.error("Error updating log status for reqId: {}: {}", event.getReqId(), e.getMessage(), e);
            return;
        }

        // Handle isLock update if applicable
        if (reqType == Integer.parseInt(properties.getProperty("isLock.request.type"))) {
            updateIsLock(mcId, triggerReference);
        } else {
            LOG.info("Request type {} does not require isLock update.", reqType);
        }
    }

    private void updateIsLock(int machineId, int lockStatus) throws SQLException {
        db2UpdateStatement.setInt(1, lockStatus);
        db2UpdateStatement.setInt(2, machineId);

        try {
            db2UpdateStatement.executeUpdate();
            db2Connection.commit();
            LOG.info("Updated isLock status for machineId: {}", machineId);
        } catch (SQLException e) {
            db2Connection.rollback();
            LOG.error("Error updating isLock status for machineId {}: {}", machineId, e.getMessage(), e);
        }
    }


    @Override
    public void close() throws Exception {
        closeResource(db1UpdateStatement);
        closeResource(db1Connection);
        closeResource(db2UpdateStatement);
        closeResource(db2Connection);
        super.close();
    }


    private void closeResource(AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                LOG.error("Error closing resource: {}", e.getMessage(), e);
            }
        }
    }
}
