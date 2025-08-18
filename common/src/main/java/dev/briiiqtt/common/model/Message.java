package dev.briiiqtt.common.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
public class Message {

    @JsonProperty("messageId")
    private String messageId;

    @JsonProperty("messageType")
    private String messageType;

    @JsonProperty("timestamp")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    private LocalDateTime timestamp;

    @JsonProperty("serverId")
    private String serverId;

    @JsonProperty("schemaVersion")
    private String schemaVersion;

    @JsonProperty("data")
    private Object data;
}