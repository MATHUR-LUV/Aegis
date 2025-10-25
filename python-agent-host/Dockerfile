
# --- STAGE 1: Build Proto Files ---
# Use the official Python protoc image as a builder
FROM python:3.11-slim as proto_builder

# Install gRPC tools
RUN pip install grpcio-tools

# Set up the working directory
WORKDIR /app

# 1. Copy the shared .proto file (now from our local context)
COPY proto/agent.proto /app/proto/

# 2. Copy and run the build script to generate Python gRPC stubs
COPY build_proto.sh .
RUN chmod +x build_proto.sh

# This script will read /app/proto/agent.proto and create Python files
RUN ./build_proto.sh

# --- THIS IS THE FIX ---
# The generated file 'agent_pb2_grpc.py' has a broken import.
# This command patches it to use a relative import.
# It changes 'import agent_pb2' to 'from . import agent_pb2'
RUN sed -i 's/^import agent_pb2 as agent__pb2/from . import agent_pb2 as agent__pb2/' /app/generated/agent_pb2_grpc.py
# --- END OF FIX ---


# --- STAGE 2: Main Application ---
# Use a slim Python image for the final application
FROM python:3.11-slim

WORKDIR /app

# Set up logging to be unbuffered (prints to console immediately)
ENV PYTHONUNBUFFERED=1

# Copy Python dependencies
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the main application code
COPY app.py .

# Copy the generated Python stubs from the 'proto_builder' stage
COPY --from=proto_builder /app/generated/ /app/generated/

# Expose the gRPC port
EXPOSE 50051

# Run the application
CMD ["python", "app.py"]

