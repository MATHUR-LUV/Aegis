package com.aegis.triage.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    // This is the core of our service.
    // This method will automatically listen to the "platform-events" topic.
    @KafkaListener(topics = "platform-events", groupId = "aegis-triage-group")
    public void handleIncident(String message) {
        log.info("Received message: {}", message);

        // TODO:
        // 1. Deserialize the JSON message into a Java Object.
        // 2. Check if the event_type is "payment_failed".
        // 3. If it is, use the gRPC client to call the Python agent.

        if (message.contains("payment_failed")) {
            log.warn("CRITICAL INCIDENT DETECTED: {}", message);
            // We will add the gRPC call here in the next step.
        }
    }
}
