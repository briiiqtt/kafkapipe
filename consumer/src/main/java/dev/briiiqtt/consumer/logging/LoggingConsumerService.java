package dev.briiiqtt.consumer.logging;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.briiiqtt.common.constant.Topics;
import dev.briiiqtt.common.model.Message;
import dev.briiiqtt.consumer.logstorage.LogStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class LoggingConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(LoggingConsumerService.class);
    private final ObjectMapper objectMapper;
    private final LogStorageService logStorageService;

    public LoggingConsumerService(ObjectMapper objectMapper, LogStorageService logStorageService) {
        this.objectMapper = objectMapper;
        this.logStorageService = logStorageService;
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
            Message message = objectMapper.readValue(jsonMessage, Message.class);

            logger.info("message parsed: {}", message.toString());

            logStorageService.addMessage(message, topic);

        } catch (Exception e) {
            logger.error("topic: {} errorMessage: {}", topic, e.getMessage());
        }
    }
}