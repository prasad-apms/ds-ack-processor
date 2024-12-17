# ds-ack-processor
Processing Data Sync Ack from the devices 

# version : v.0.0.4
# Date : 17-12-2024

1. Issue: After update the req_id in the rock db after some time interval need to clear the data from rock.clear the data from the rock db issue resolved and interval maintained in the properties file.


# version : v.0.0.3
# Date : 16-12-2024

1. Based on the request type ,verify the data, if is_lock type update the lock status in the  mc_machine table.
2. while updating the acknowledge the data in the sync_log table , update the total duration time of the process of completion of the request.
3. For update the is_lock status in the mc_machine table, established the new db2 connection.
4. For db connection established , used the hikari connection pool method.

# version : v.0.0.1
# Date : 04-12-2024

1. consume ack data from the kafka topic and insert into the sync log in postgres db.
2. check if the ack data is valid and if the data is not valid, send an error
