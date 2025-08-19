package dev.briiiqtt.consumer.bucket;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.briiiqtt.common.constant.Topics;
import dev.briiiqtt.common.model.Message;
import dev.briiiqtt.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class BucketConsumer extends Consumer {
    private static final String GROUP_ID = "message-to-bucket";
    private final MessageToBucketService messageToBucketService;

    public BucketConsumer(ObjectMapper objectMapper, MessageToBucketService messageToBucketService) {
        super(objectMapper);
        this.messageToBucketService = messageToBucketService;
    }

    @KafkaListener(topics = Topics.USER_ACTION, groupId = GROUP_ID)
    public void handleUserAction(String message) {
        processMessage(message, Topics.USER_ACTION);
    }

    @KafkaListener(topics = Topics.SYSTEM_LOG, groupId = GROUP_ID)
    public void handleSystemLog(String message) {
        processMessage(message, Topics.SYSTEM_LOG);
    }

    protected void processMessage(String jsonMessage, String topic) {
        try {
            Message message = objectMapper.readValue(jsonMessage, Message.class);

            logger.info("message parsed: {}", message.toString());

            messageToBucketService.addMessage(message, topic);

        } catch (Exception e) {
            logger.error("topic: {} errorMessage: {}", topic, e.getMessage());
        }
    }
}