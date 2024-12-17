package org.flink.utils;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;

import org.flink.DsAckToLogJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Helper {

    private static final Logger logger = LoggerFactory.getLogger(Helper.class);

    public static String convertToSessionDuration(Timestamp start, Timestamp end){

        long durationMillis =   start.getTime() - end.getTime();
        Duration duration = Duration.ofMillis(durationMillis);
        return String.format("%02d:%02d:%02d", Math.abs(duration.toHours()), Math.abs(duration.toMinutes()) % Constants.MIN_IN_SECONDS, Math.abs(duration.getSeconds()) % Constants.MIN_IN_SECONDS);

    }

    /**
     * Validate the required properties are present and not empty.
     */
    public static void validateProperties(Properties properties) {
        if (properties.getProperty("BROKERS") == null || properties.getProperty("KAFKA_TOPIC_CONSUMER") == null) {
            logger.error("Required Kafka properties are missing. Please check the configuration.");
            throw new IllegalArgumentException("Missing required Kafka properties.");
        }
    }
}
