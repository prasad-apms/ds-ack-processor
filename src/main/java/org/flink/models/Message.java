package org.flink.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAlias;
import java.sql.Timestamp;
import lombok.Getter;
import lombok.Setter;

/**
 * Deserialized the kafka message  mapped in the POJO.
 * Handles both flat and nested versions, handle all message structure using jackson annotation s
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class Message {


    //Kafka metadata information
    private int partition;
    private long offset;
    private String topic;
    private String key;
    private long timestamp;
    private String timestampType;
    private Timestamp shiftTerminalTime;
    private String duration;


    // Message license information
    @JsonAlias("structure_id")
    private String structureId;
    
    @JsonAlias("source_id")
    private String sourceId;

     @JsonAlias("license_id")
    private String licenseId;

    @JsonAlias("expiry_date")
    private String expiryDate;

    
    @JsonAlias({"req_id","reqId"})
    private String reqId;



}