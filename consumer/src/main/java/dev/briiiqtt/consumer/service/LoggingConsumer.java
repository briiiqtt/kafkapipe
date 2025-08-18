package dev.briiiqtt.consumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.briiiqtt.common.constants.Topics;
import dev.briiiqtt.common.model.LogMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class LoggingConsumer {

    private static final Logger logger = LoggerFactory.getLogger(LoggingConsumer.class);

    private final ObjectMapper objectMapper;

    public LoggingConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = Topics.USER_ACTION, groupId = "group-1")
    public void handleUserAction(String message) {
        processLogMessage(message, Topics.USER_ACTION);
    }

    @KafkaListener(topics = Topics.SYSTEM_LOG, groupId = "group-1")
    public void handleSystemLog(String message) {
        processLogMessage(message, Topics.SYSTEM_LOG);
    }

    private void processLogMessage(String jsonMessage, String topic) {
        try {
            LogMessage logMessage = objectMapper.readValue(jsonMessage, LogMessage.class);

            // TODO: 가져온 메시지 처리 로직

        } catch (Exception e) {
            logger.error("topic: {} errorMessage: {}", topic, e.getMessage());
        }
    }
}