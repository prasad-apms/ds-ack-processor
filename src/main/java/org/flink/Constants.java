package org.flink;


public final class Constants {
    public static final int MIN_IN_SECONDS = 60;
    public static final int HOUR_IN_SECONDS = 3600;

    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    // Machine status codes
    public static final String MC_STOP_STATUS = "0";
    public static final String MC_RUN_STATUS = "1";
    public static final String MC_IDLE_STATUS = "2";
    public static final String MC_OFF_STATUS = "3";
    public static final String MC_MNT_STATUS = "4";

    // Private constructor to prevent instantiation
    private Constants() {
        throw new UnsupportedOperationException("This is a constants class and cannot be instantiated");
    }
}
