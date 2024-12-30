package org.flink.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class SinkData {
	private String terminalTime ;
	private String machineId;
	private String isLock;


    //Health_Exceed , Machine_Status ,changeOverStatus
	private String status; 

    // Method to populate the Notification object with data from the Message object
    public void populateFromMessage(String terminalTime,int isLock,int mcId, String payloadKey) {
        this.status = payloadKey;
        this.machineId = String.valueOf(mcId);
        this.terminalTime = terminalTime;
        this.isLock = String.valueOf(isLock);
    }

    // Method to convert the populated Notification object to a JSON string
    public String toJsonString() throws JsonProcessingException {
        final ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(this);
    }
}
