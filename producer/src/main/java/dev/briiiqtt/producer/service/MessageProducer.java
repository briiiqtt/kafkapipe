package dev.briiiqtt.producer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.briiiqtt.common.constants.Topics;
import dev.briiiqtt.common.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class MessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public MessageProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendLogMessage(Message message) {
        try {
            String topic = getTopicByLogType(message.getMessageType());
            String jsonMessage = objectMapper.writeValueAsString(message);

            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(topic, jsonMessage);

            future.whenComplete((result, exception) -> {
                if (exception == null) {
                    logger.info("produce succeed on topic: {}", topic);
                } else {
                    logger.error("topic {}: {}", topic, exception.getMessage());
                }
            });

        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    private String getTopicByLogType(String logType) {
        return switch (logType) {
            case "user-action" -> Topics.USER_ACTION;
            case "system-log" -> Topics.SYSTEM_LOG;
            default -> Topics.SYSTEM_LOG;
        };
    }
}