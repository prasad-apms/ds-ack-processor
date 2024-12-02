package org.flink;

import java.time.*;
import java.time.format.DateTimeFormatter;

public class TimeZoneConvert {
    //private static final Logger logger = LoggerFactory.getLogger(TimeZoneConvert.class);

    public static ZoneOffset offTimeZoneConvert(String utcOff) {

        // Validate input
        if (utcOff == null || utcOff.isEmpty()) {
            throw new IllegalArgumentException("Invalid UTC offset: " + utcOff);
        }
        // Parse the UTC offset string to extract hours and minutes
        String[] parts = utcOff.split("[+\\-:]");
        int hours = Integer.parseInt(parts[1]); // Extract hours part
        int minutes = Integer.parseInt(parts[2]); // Extract minutes part

        // Determine the sign of the offset (+ or -)
        boolean isPositive = utcOff.startsWith("UTC+");

        // Calculate the total seconds for the offset
        int totalSeconds = (hours * Constants.HOUR_IN_SECONDS + minutes * Constants.MIN_IN_SECONDS) * (isPositive ? 1 : -1);

        // Create a ZoneOffset using the calculated total seconds
        return ZoneOffset.ofTotalSeconds(totalSeconds);
    }

    public static String convertToUTC(String terminal_time , String timezone,int UTC_Date_Time) {

        // Define your local datetime string
        LocalDateTime localDateTime;

        try {
            // Parse the local datetime string into LocalDateTime
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            localDateTime = LocalDateTime.parse(terminal_time, formatter);

        }catch (Exception e){
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
                localDateTime = LocalDateTime.parse(terminal_time, formatter);
            }catch(Exception a){
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.DATE_TIME_FORMAT);
                localDateTime = LocalDateTime.parse(terminal_time, formatter);
            }
        }

        // Define your local time zone (UTC+05:30)
        ZoneOffset localZoneOffset = offTimeZoneConvert(timezone);

        //logger.info("ZoneOffset: {}" , localZoneOffset);
        // Convert LocalDateTime to ZonedDateTime
        ZonedDateTime localZonedDateTime = ZonedDateTime.of(localDateTime, localZoneOffset);

        // Convert to UTC (Coordinated Universal Time)
        ZonedDateTime utcZonedDateTime = localZonedDateTime.withZoneSameInstant(ZoneOffset.UTC);

        if(UTC_Date_Time ==1) {
            // Format UTC ZonedDateTime into desired string format
            DateTimeFormatter utcFormatter = DateTimeFormatter.ofPattern(Constants.DATE_TIME_FORMAT);

            //logger.info("UTC DateTime of Unit: " + utcDateTimeString);
            return utcZonedDateTime.format(utcFormatter);

        }else if(UTC_Date_Time == 0){

            // Format UTC ZonedDateTime into desired string format (date only)
            DateTimeFormatter utcDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

            // String utcDateTimeString = utcZonedDateTime.format(utcDateFormatter); // logger.info("UTC Date of Unit: " + utcZonedDateTime.format(utcDateFormatter));
            return utcZonedDateTime.format(utcDateFormatter);

        }

        return terminal_time;
    }


    public static String convertUTC_Local(String utcTimeString ,String offset){
        // Parse the input UTC time string to LocalDateTime
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
        LocalDateTime localDateTime = LocalDateTime.parse(utcTimeString, inputFormatter);

        // Assume the input time is in UTC
        OffsetDateTime utcOffsetDateTime = localDateTime.atOffset(ZoneOffset.UTC);

        // Define the target timezone (UTC+05:30)
        ZoneId targetZone = ZoneId.of(offset);

        // Convert the UTC OffsetDateTime to the target timezone
        OffsetDateTime localOffsetDateTime = utcOffsetDateTime.atZoneSameInstant(targetZone).toOffsetDateTime();

        // Format the output to the same pattern
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        return localOffsetDateTime.format(outputFormatter);
    }
}
