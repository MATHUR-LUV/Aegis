package com.aegis.triage.service;

import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

// Import the generated gRPC classes
// The 'build.gradle' plugin will create these automatically
import com.aegis.proto.AgentServiceGrpc;
import com.aegis.proto.IncidentRequest;
import com.aegis.proto.IncidentResponse;

@Slf4j
@Service
public class KafkaConsumerService {

    // Inject the gRPC client stub, pointing to the 'agent-service'
    // configuration in our application.properties
    @GrpcClient("agent-service")
    private AgentServiceGrpc.AgentServiceBlockingStub agentClient;

    @KafkaListener(topics = "platform-events", groupId = "aegis-triage")
    public void consume(String message) {
        log.info("Received message: {}", message);

        // Our original incident detection logic
        if (message.contains("payment_failed")) {

            // --- This is the new gRPC Logic ---
            try {
                log.warn("CRITICAL INCIDENT DETECTED. Triggering AI Agent...");

                // 1. Build the gRPC request message from our .proto file
                IncidentRequest request = IncidentRequest.newBuilder()
                        .setEventType("payment_failed")
                        .setFullEventJson(message) // Send the full event
                        .build();

                // 2. Make the blocking gRPC call to the Python service
                IncidentResponse response = agentClient.handleIncident(request);

                // 3. Log the AI agent's response
                log.warn("AI AGENT RESPONSE (Status: {}): {}",
                        response.getStatus(),
                        response.getAgentResponse());

            } catch (Exception e) {
                // If the gRPC call fails, log it but don't crash the consumer
                log.error("Failed to call AI Agent (gRPC): {}", e.getMessage());
            }
            // --- End of new gRPC Logic ---
        }
    }
}





// package com.aegis.triage.service;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.kafka.annotation.KafkaListener;
// import org.springframework.stereotype.Service;

// @Service
// public class KafkaConsumerService {

//     private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

//     // This is the core of our service.
//     // This method will automatically listen to the "platform-events" topic.
//     @KafkaListener(topics = "platform-events", groupId = "aegis-triage-group")
//     public void handleIncident(String message) {
//         log.info("Received message: {}", message);

//         // TODO:
//         // 1. Deserialize the JSON message into a Java Object.
//         // 2. Check if the event_type is "payment_failed".
//         // 3. If it is, use the gRPC client to call the Python agent.

//         if (message.contains("payment_failed")) {
//             log.warn("CRITICAL INCIDENT DETECTED: {}", message);
//             // We will add the gRPC call here in the next step.
//         }
//     }
// }
