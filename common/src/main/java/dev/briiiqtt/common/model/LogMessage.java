package dev.briiiqtt.common.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

public class LogMessage {

    @JsonProperty("logType")
    private String logType;

    @JsonProperty("timestamp")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    private LocalDateTime timestamp;

    @JsonProperty("serverId")
    private String serverId;

    @JsonProperty("data")
    private Object data;

    public LogMessage() {
    }

    public LogMessage(String logType, String serverId, Object data) {
        this();
        this.logType = logType;
        this.serverId = serverId;
        this.data = data;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}