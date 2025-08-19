package dev.briiiqtt.consumer.logstorage;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.briiiqtt.common.model.Message;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class LogStorageService {

    private static final Logger logger = LoggerFactory.getLogger(LogStorageService.class);
    private static final String BUCKET_NAME = "kafka-message";

    private final MinioClient minioClient;
    private final ObjectMapper objectMapper;
    private final Map<String, StringBuilder> topicBuffers = new ConcurrentHashMap<>();
    private final Map<String, Integer> messageCount = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public LogStorageService(MinioClient minioClient, ObjectMapper objectMapper) {
        this.minioClient = minioClient;
        this.objectMapper = objectMapper;
        initializeBucket();
        startPeriodicFlush();
    }

    @PostConstruct
    private void initializeBucket() {
        try {
            boolean found = minioClient.bucketExists(BucketExistsArgs.builder()
                    .bucket(BUCKET_NAME)
                    .build());

            if (!found) {
                minioClient.makeBucket(MakeBucketArgs.builder()
                        .bucket(BUCKET_NAME)
                        .build());
                logger.info("bucket created: {}", BUCKET_NAME);
            }
        } catch (Exception e) {
            throw new RuntimeException("bucket initialization failed", e);
        }
    }

    public void addMessage(Message message, String topic) {
        try {
            String jsonLine = objectMapper.writeValueAsString(message) + "\n";

            topicBuffers.computeIfAbsent(topic, k -> new StringBuilder())
                    .append(jsonLine);
            messageCount.merge(topic, 1, Integer::sum);

            // 버퍼 크기 체크 (예: 1MB 또는 1000개 메시지)
            if (shouldFlush(topic)) {
                flushBuffer(topic);
            }

            logger.info("message added: {}", message.toString());

        } catch (Exception e) {
            logger.error("Failed to add message to buffer: {}", e.getMessage());
        }
    }

    private boolean shouldFlush(String topic) {
        StringBuilder buffer = topicBuffers.get(topic);
        Integer count = messageCount.get(topic);

        return buffer != null &&
                (buffer.length() > 1024 * 1024 || // 1MB
                        count != null && count >= 1000); // 1000개 메시지
    }

    private void startPeriodicFlush() {
        scheduler.scheduleAtFixedRate(() -> {
            topicBuffers.keySet().forEach(this::flushBuffer);
        }, 60, 60, TimeUnit.SECONDS); // 60초마다
    }

    private synchronized void flushBuffer(String topic) {
        StringBuilder buffer = topicBuffers.get(topic);
        Integer count = messageCount.get(topic);

        if (buffer == null || buffer.length() == 0) {
            return;
        }

        try {
            String objectName = generateJSONLObjectName(topic);
            byte[] content = buffer.toString().getBytes(StandardCharsets.UTF_8);

            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(BUCKET_NAME)
                    .object(objectName)
                    .stream(new ByteArrayInputStream(content), content.length, -1)
                    .contentType("application/x-ndjson") // JSONL MIME type
                    .build());

            logger.info("Stored JSONL to MinIO: {} ({} messages)", objectName, count);

            // 버퍼 초기화
            buffer.setLength(0);
            messageCount.put(topic, 0);

        } catch (Exception e) {
            logger.error("Failed to flush buffer to MinIO: {}", e.getMessage());
        }
    }

    private String generateJSONLObjectName(String topic) {
        LocalDateTime now = LocalDateTime.now();
        String datePath = now.format(DateTimeFormatter.ofPattern("yyyy/MM/dd/HH"));
        String timestamp = now.format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));

        return String.format("%s/%s/%s.jsonl", topic, datePath, timestamp);
    }
}

