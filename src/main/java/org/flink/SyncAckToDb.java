package org.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class SyncAckToDb extends RichSinkFunction<Message> {
    private static final Logger LOG = LoggerFactory.getLogger(SyncAckToDb.class);
    private final String logAckUpdateQry = "UPDATE dw.log_mc_ed_sync SET status = 1 , duration = ?::interval  WHERE sync_id = ?" ;
    
    private final String isLockUpdateQry = "UPDATE mc_machines SET is_lock = ? WHERE machine_id = ?" ;
    
    private final String fetchReqLogDataQry = "SELECT trigger_type AS reqType, trigger_ref AS tRef, machine_id AS mcId ,cdt as startTime,udt as endTime " +
    "FROM dw.log_mc_ed_sync WHERE sync_id = ? limit 1";

    private transient Connection db1Connection;
    private transient PreparedStatement db1PreparedStatement;

    private transient Connection db2Connection;
    private transient PreparedStatement db2PreparedStatement;


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
        db1Connection = DatabaseConnectionManager.getDB1Connection();

        db1PreparedStatement = db1Connection.prepareStatement(logAckUpdateQry);

        // Disable auto-commit for batch execution (postgres)
        db1Connection.setAutoCommit(false);

        //db2 connection
        Class.forName("com.mysql.cj.jdbc.Driver");

        db2Connection = DatabaseConnectionManager.getDB2Connection();
        db2PreparedStatement = db2Connection.prepareStatement(isLockUpdateQry);

        db2Connection.setAutoCommit(false);


    }

    @Override
    public void invoke(Message event, Context context)  {
        if (event != null) {
            // if the request type is isLock , update the lock status in the db2 connection.
            try{

                checkRequestType(event);
            }catch(Exception e){
                LOG.error("msg-Ack exception : {}", e);
            }    
        } 
    }

    private void updateIsLock( int mcId , int tRef) throws SQLException{
        try{
            LOG.info("Updating the isLock status in the dB2 of mc_machines !");
            // Set the parameter for the prepared statement
            db2PreparedStatement.setInt(1, tRef);
            db2PreparedStatement.setInt(2, mcId);
            db2PreparedStatement.executeUpdate();
            db2Connection.commit();   
        }catch(SQLException e){
            db2Connection.rollback();
            LOG.error(" Db2 Roll backed :  updateIsLock error :  {}", e.toString());
        }
            
    }

    private void checkRequestType(Message event){

        // Use try-with-resources to ensure proper resource cleanup
        try (PreparedStatement reqLogDataPs = db1Connection.prepareStatement(fetchReqLogDataQry)) {
            // Set the parameter for the prepared statement
            reqLogDataPs.setString(1, event.getReqId());

            // Execute the query
            try (ResultSet opResult = reqLogDataPs.executeQuery()) {

                // Process the result set
                if (opResult.next()) {
                    int reqType = opResult.getInt("reqType");
                    int tRef = opResult.getInt("tRef");
                    int mcId = opResult.getInt("mcId");
                    Timestamp startTime = opResult.getTimestamp("startTime");
                    Timestamp endTime = opResult.getTimestamp("endTime");
                    String duration = Helper.convertToSessionDuration(startTime, endTime);

                    try{
                        LOG.info("updating the status flag of req-id {}",event.getReqId());

                        // update the log status and duration.
                        db1PreparedStatement.setString(1, duration);
                        db1PreparedStatement.setString(2, event.getReqId());
                        db1PreparedStatement.executeUpdate();
                        db1Connection.commit();
                                
                    }catch(SQLException e){
                        db1Connection.rollback();
                        LOG.error("Error in request id {}", event.getReqId());
                        LOG.error("dB1 Roll backed :  update status and duration error :  {}", e.toString());
                        return;
                    }
                   
                    // After updating the status and duration , check the request type if isLock update the lock status in the db2 mc_machine.
                    if (reqType ==  Integer.parseInt(props.getProperty("isLock.request.type"))) {
                       updateIsLock(mcId, tRef);
                    }else{
                        LOG.info("Skip the process, request type as {}", reqType);
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error("Error fetching request log data: ", e.toString());
            // Handle SQL exceptions appropriately
            e.printStackTrace(); // Consider using a logger for better error handling
        }
    }

    @Override
    public void close() throws Exception {
        if (db1PreparedStatement != null) {
            db1PreparedStatement.close();
        }
        if (db1Connection != null) {
            db1Connection.close();
        }

        if (db2PreparedStatement != null) {
            db2PreparedStatement.close();
        }
        if (db2Connection != null) {
            db2Connection.close();
        }
        super.close();
    }
}
