package org.flink;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.slf4j.LoggerFactory;
import java.io.FileReader;
import org.slf4j.Logger;
import java.util.*;

/**
 * This project executes the Flink environment to communicate with the
 * Kafka message broker, consumes messages from Kafka, processes the data and update the acknowledge to the sync log.
 **/

public class App {


    private static final Logger LOG = LoggerFactory.getLogger(App.class);
    
    // Kafka source configuration properties
    private static final  Properties kafkaConsumerProperties(Properties prop,String appName) {

        Properties kafkaProps = new Properties();

        kafkaProps.setProperty("security.protocol", prop.getProperty("SECURITY_PROTOCOL"));
        kafkaProps.setProperty("sasl.mechanism", prop.getProperty("SASL_MECHANISM"));

        // Consumer Properties for High Throughput
        kafkaProps.setProperty("max.poll.records", "10000");
        kafkaProps.setProperty("fetch.min.bytes", "1048576"); // 1MB
        kafkaProps.setProperty("fetch.max.wait.ms", "500");   // 0.5 seconds
        kafkaProps.setProperty("max.partition.fetch.bytes", "2097152"); // 2MB
        kafkaProps.setProperty("session.timeout.ms", "60000");    // 60 seconds
        kafkaProps.setProperty("heartbeat.interval.ms", "20000"); // 20 seconds
        kafkaProps.setProperty("request.timeout.ms", "305000");   // Just above session timeout

        // Disable auto-commit, and manually commit offsets
        kafkaProps.setProperty("enable.auto.commit", "false");

        kafkaProps.setProperty("sasl.jaas.config",
            String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';", 
                prop.getProperty("KAFKA_USERNAME"), 
                prop.getProperty("KAFKA_PASSWORD")
        ));

        // Configure the path of truststore if client authentication is required
        kafkaProps.setProperty("ssl.truststore.location", prop.getProperty("SSL_TRUSTSTORE_LOCATION"));
        kafkaProps.setProperty("ssl.truststore.password", prop.getProperty("SSL_TRUSTSTORE_PASSWORD"));

        // Configure the path of keystore (private key) if client authentication is
        // required
        kafkaProps.setProperty("ssl.keystore.location", prop.getProperty("SSL_KEYSTORE_LOCATION"));
        kafkaProps.setProperty("ssl.keystore.password", prop.getProperty("SSL_KEYSTORE_PASSWORD"));
        
        return kafkaProps;
    }

    /**
     * Created a new Flink environment with Kafka consumer (data source) 
     * (data pipeline for update the acknowledge the sync log) .
     * direct add sink interaction parallelism set as 2.
     */

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Machines Data Pipeline....");

        if (args.length != 1) {
            LOG.error("Usage: App <properties_file_path>");
            System.exit(1);
        }

        // Extract the configuration file path from the arguments array and create a new
        String propertiesFilePath  = args[0];
        LOG.info("Properties file path: {}", propertiesFilePath);

        // Load the properties file
        final Properties properties = new Properties();
        properties.load(new FileReader(propertiesFilePath));

        // Job Variables
        int parallelism = Integer.parseInt(properties.getProperty("PARALLELISM"));
        String jobName =  properties.getProperty("JOB_NAME");
        String appName =  properties.getProperty("app.environment");
        
        // Kafka Parameters
        String BROKERS = properties.getProperty("BROKERS");
        String PARTITION_INTERVALS = properties.getProperty("PARTITION_INTERVALS");
        String MACHINE_INSTANCE = properties.getProperty("KAFKA_TOPIC_CONSUMER");
        String KAFKA_GROUP_ID = properties.getProperty("KAFKA_GROUP_ID");

        if("dev".equals(appName)){
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Kolkata"));
        }

        // Create a new Flink environment with Kafka consumer with parallelism
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // set a parallelism in the environment
        environment.setParallelism(parallelism);

        // ***************************** Establish the Kafka consumer *************************************************** //
        KafkaSource<Message> source = KafkaSource.<Message>builder()
            .setBootstrapServers(BROKERS)
            .setProperty("partition.discovery.interval.ms", PARTITION_INTERVALS)
            .setTopics(MACHINE_INSTANCE)
            .setGroupId(KAFKA_GROUP_ID)
            .setProperties(kafkaConsumerProperties(properties,appName))
            //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(new MessageDeserialization())
            .build();

        // Consuming a message from Kafka
        DataStream<Message> streamingData = environment.fromSource(source, WatermarkStrategy.noWatermarks(), "ackKafka");

        // Keyed Streaming, based on key process the machine state. //
        DataStream<Message> msgKeyedStream = streamingData
            .keyBy(Message::getReqId)
            .process(new MsgCheckerStream()) 
            .name("Keyed Stream of acknowledgment");
       

        // Add the custom sink
        msgKeyedStream.addSink(new SyncAckToDb(properties));

        LOG.info("Started Successfully : JOB NAME : {}",jobName);
        environment.execute(jobName);

 
    }  

}
