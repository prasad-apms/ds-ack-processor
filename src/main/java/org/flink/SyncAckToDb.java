package org.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Properties;

public class SyncAckToDb extends RichSinkFunction<Message> {

    private final String updateQuery = "UPDATE dw.log_mc_ed_sync SET status = 1 WHERE sync_id = ?" ;

    private transient Connection connection;
    private transient PreparedStatement preparedStatement;

    private final Properties props;

    public SyncAckToDb(Properties props){
        this.props = props;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Ensure the driver class is loaded
        Class.forName("org.postgresql.Driver");

         // Initialize the connection pool
         DatabaseConnectionManager.initialize(props);

        // Get shared connection
        connection = DatabaseConnectionManager.getConnection();
        preparedStatement = connection.prepareStatement(updateQuery);

        // Disable auto-commit for batch execution (postgres)
        connection.setAutoCommit(false);
    }

    @Override
    public void invoke(Message event, Context context) throws Exception {
        if (event != null) {
            // Set the parameter and execute
            preparedStatement.setString(1, event.getReqId());
            preparedStatement.executeUpdate();
            connection.commit();
        }
        
    }

    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }
}
