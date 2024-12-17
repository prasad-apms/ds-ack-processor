package org.flink;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.flink.models.Message;
import org.flink.sink.SyncAckToDb;
import org.flink.stream.DsAckStream;
import org.flink.utils.Helper;
import org.flink.utils.MessageDeserialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.TimeZone;


/**
 * This project executes the Flink environment to communicate with the
 * Kafka message broker, consumes messages from Kafka, processes the data and update the acknowledge to the sync log.
 **/

public class DsAckToLogJob {

    private static final Logger logger = LoggerFactory.getLogger(DsAckToLogJob.class);
    
    /**
     * Load Kafka consumer properties from the configuration.
     */
    private static Properties loadKafkaConsumerProperties(Properties configProperties,String appName) {

        Properties kafkaProps = new Properties();

        kafkaProps.setProperty("security.protocol", configProperties.getProperty("SECURITY_PROTOCOL"));
        kafkaProps.setProperty("sasl.mechanism", configProperties.getProperty("SASL_MECHANISM"));
        kafkaProps.setProperty("max.poll.records", "10000");
        kafkaProps.setProperty("fetch.min.bytes", "1048576"); // 1MB
        kafkaProps.setProperty("fetch.max.wait.ms", "500");   // 0.5 seconds
        kafkaProps.setProperty("max.partition.fetch.bytes", "2097152"); // 2MB
        kafkaProps.setProperty("session.timeout.ms", "60000");    // 60 seconds
        kafkaProps.setProperty("heartbeat.interval.ms", "20000"); // 20 seconds
        kafkaProps.setProperty("request.timeout.ms", "305000");   // Just above session timeout
        kafkaProps.setProperty("enable.auto.commit", "false"); // Disable auto-commit, and manually commit offsets

        kafkaProps.setProperty("sasl.jaas.config", String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';",
            configProperties.getProperty("KAFKA_USERNAME"),
            configProperties.getProperty("KAFKA_PASSWORD")
        ));

        kafkaProps.setProperty("ssl.truststore.location", configProperties.getProperty("SSL_TRUSTSTORE_LOCATION"));
        kafkaProps.setProperty("ssl.truststore.password", configProperties.getProperty("SSL_TRUSTSTORE_PASSWORD"));
        kafkaProps.setProperty("ssl.keystore.location", configProperties.getProperty("SSL_KEYSTORE_LOCATION"));
        kafkaProps.setProperty("ssl.keystore.password", configProperties.getProperty("SSL_KEYSTORE_PASSWORD"));
        
        return kafkaProps;
    }

    /**
     * Main method to set up Flink environment and execute the job.
     * Created a new Flink environment with Kafka consumer (data source) (data pipeline for update the acknowledge the sync log) .
     * direct add sink interaction parallelism set as 2.
     */

    public static void main(String[] args) throws Exception {
        logger.info("Starting Machines Data Pipeline....");

        if (args.length != 1) {
            logger.error("Usage: DsAckToLogJob <properties_file_path>");
            return;
        }

        String propertiesFilePath  = args[0];
        logger.info("Properties file path: {}", propertiesFilePath);

        Properties properties = new Properties();
        try {
            properties.load(new FileReader(propertiesFilePath));
        } catch (IOException e) {
            logger.error("Failed to load properties file: {}", propertiesFilePath, e);
            return;
        }

        // Validate required properties
        Helper.validateProperties(properties);

        // Job Variables
        int parallelism = Integer.parseInt(properties.getProperty("PARALLELISM","1"));
        String jobName =  properties.getProperty("JOB_NAME","DefaultJob");
        String appName =  properties.getProperty("app.environment");
        
        // Kafka Parameters
        String broker = properties.getProperty("BROKERS");
        String partitionIntervals = properties.getProperty("PARTITION_INTERVALS");
        String consumerTopicName = properties.getProperty("KAFKA_TOPIC_CONSUMER");
        String kGroupId = properties.getProperty("KAFKA_GROUP_ID");

        // Set timezone for specific app environment
        if("dev".equals(appName)){
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Kolkata"));
        }

        // Create a new Flink environment with Kafka consumer with parallelism
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(parallelism);

        // ********************** Establish the Kafka consumer ***********************//
        KafkaSource<Message> source = KafkaSource.<Message>builder()
            .setBootstrapServers(broker)
            .setProperty("partition.discovery.interval.ms", partitionIntervals)
            .setTopics(consumerTopicName)
            .setGroupId(kGroupId)
            .setProperties(loadKafkaConsumerProperties(properties,appName))
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            //.setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(new MessageDeserialization())
            .build();

        // Consuming a message from Kafka
        DataStream<Message> messageStream  = environment.fromSource(source, WatermarkStrategy.noWatermarks(), "ackKafka");

        // Keyed Streaming, based on key process the machine state.
        DataStream<Message> keyedStream = messageStream 
            .keyBy(Message::getReqId)
            .process(new DsAckStream(properties)) 
            .name("Keyed Stream of acknowledgment");
       

        // Add the custom sink
        keyedStream.addSink(new SyncAckToDb(properties));

        logger.info("Started Successfully : JOB NAME : {}",jobName);
        
        // Execute the Flink job
        try {
            environment.execute(jobName);
        } catch (Exception e) {
            logger.error("Job execution failed", e.toString());
        }
    }  
}
