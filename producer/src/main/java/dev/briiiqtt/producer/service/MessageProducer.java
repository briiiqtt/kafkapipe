package dev.briiiqtt.producer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.briiiqtt.common.constant.Topics;
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

    public void sendMessage(Message message) {
        try {
            String topic = getTopicByMessageType(message.getMessageType());
            String jsonMessage = objectMapper.writeValueAsString(message);

            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(topic, jsonMessage); // key나 partition 지정 없이 round-robin: 로그를 저장하기만 할거라 순서 보장될 필요 없음

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

    private String getTopicByMessageType(String messageType) {
        return switch (messageType) {
            case "user-action" -> Topics.USER_ACTION;
            case "system-log" -> Topics.SYSTEM_LOG;
            default -> throw new RuntimeException("no topic for messageType: " + messageType);
        };
    }
}