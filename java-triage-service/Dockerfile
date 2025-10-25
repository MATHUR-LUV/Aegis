
# --- STAGE 1: Build the application ---
FROM eclipse-temurin:17-jdk-jammy as builder
WORKDIR /app

# Copy the Gradle wrapper
COPY gradlew .
COPY gradle ./gradle

# Copy the build files
COPY build.gradle .
COPY settings.gradle .

# Copy the source code
COPY src ./src

# --- THIS IS THE FIX ---
# We must also copy the proto directory
COPY proto ./proto

# --- END OF FIX ---

# Build the application
# This will compile the code and create the executable jar
RUN ./gradlew build -x test --no-daemon

# --- STAGE 2: Create the final lightweight image ---
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

# Copy the built jar from the 'builder' stage
COPY --from=builder /app/build/libs/java-triage-service-0.0.1-SNAPSHOT.jar app.jar

# Expose the port (though not strictly needed for a Kafka consumer)
EXPOSE 8080

# Run the application
CMD ["java", "-jar", "app.jar"]
