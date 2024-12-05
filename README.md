# ds-ack-processor
Processing Data Sync Ack from the devices 

# version : v.0.0.1
# Date : 04-12-2024

1. consume ack data from the kafka topic and insert into the sync log in postgres db.
2. check if the ack data is valid and if the data is not valid, send an error
