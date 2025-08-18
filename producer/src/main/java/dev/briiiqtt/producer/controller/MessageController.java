package dev.briiiqtt.producer.controller;

import dev.briiiqtt.common.model.LogMessage;
import dev.briiiqtt.producer.service.MessageProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/log")
public class MessageController {

    private final MessageProducer messageProducer;

    public MessageController(MessageProducer messageProducer) {
        this.messageProducer = messageProducer;
    }

    @PostMapping
    public ResponseEntity<String> sendLog(@RequestBody LogMessage logMessage) {
        if (false /*TODO: 조건설정*/) {
            return ResponseEntity.badRequest().build();
        }

        messageProducer.sendLogMessage(logMessage);
        return ResponseEntity.ok(""/*TODO: response 형식 정의*/);
    }
}