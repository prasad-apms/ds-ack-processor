package org.flink;

import java.sql.Timestamp;
import java.time.Duration;

public class Helper {
    

        public static String convertToSessionDuration(Timestamp start, Timestamp end){

            long durationMillis =   start.getTime() - end.getTime();
            Duration duration = Duration.ofMillis(durationMillis);
            return String.format("%02d:%02d:%02d", Math.abs(duration.toHours()), Math.abs(duration.toMinutes()) % Constants.MIN_IN_SECONDS, Math.abs(duration.getSeconds()) % Constants.MIN_IN_SECONDS);

    }
}
