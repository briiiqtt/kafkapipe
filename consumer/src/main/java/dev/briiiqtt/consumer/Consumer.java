package dev.briiiqtt.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.briiiqtt.consumer.bucket.BucketConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Consumer {
    protected static final Logger logger = LoggerFactory.getLogger(BucketConsumer.class);
    protected final ObjectMapper objectMapper;

    public Consumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    protected abstract void processMessage(String jsonMessage, String topic);
}