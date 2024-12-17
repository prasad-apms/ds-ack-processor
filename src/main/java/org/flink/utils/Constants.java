package org.flink.utils;
public final class Constants {
    public static final int MIN_IN_SECONDS = 60;
    public static final int HOUR_IN_SECONDS = 3600;

    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    // Private constructor to prevent instantiation
    private Constants() {
        throw new UnsupportedOperationException("This is a constants class and cannot be instantiated");
    }
}
