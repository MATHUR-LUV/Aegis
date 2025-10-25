import grpc
import ollama
from concurrent import futures
import time
import logging
import os

# Import the generated gRPC files from the 'generated' sub-directory
from generated import agent_pb2
from generated import agent_pb2_grpc

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get the host for Ollama (defaults to host.docker.internal)
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://host.docker.internal:11434")


class AgentService(agent_pb2_grpc.AgentServiceServicer):
    """Implements the gRPC AgentService."""

    def __init__(self):
        self.ollama_client = None
        self.connect_to_ollama()

    def connect_to_ollama(self):
        """Connects to the Ollama client."""
        try:
            logging.info(f"Connecting to Ollama at {OLLAMA_HOST}...")
            self.ollama_client = ollama.Client(host=OLLAMA_HOST)
            # --- THIS IS THE FIX ---
            # We just list the models to confirm connection, we don't try to pull.
            # This assumes you have run 'ollama pull llama3' manually on your host.
            self.ollama_client.list()
            # --- END OF FIX ---
            logging.info("Ollama client initialized and ready.")
        except Exception as e:
            logging.error(f"Failed to connect to Ollama: {e}")
            logging.error("Please ensure Ollama is running on your host machine")
            self.ollama_client = None

    def HandleIncident(self, request, context):
        """Handles the incoming gRPC request from the Java service."""
        logging.info(f"Received incident from Java: {request.event_type}")

        if not self.ollama_client:
            error_msg = "Ollama client not initialized. Check connection."
            logging.error(error_msg)
            return agent_pb2.IncidentResponse(status="ERROR", agent_response=error_msg)

        try:
            # 1. Create the prompt for the LLM
            prompt = f"""
            You are 'Aegis', an autonomous incident resolution agent.
            You have received a critical incident:
            - Event Type: {request.event_type}
            - Full Event JSON: {request.full_event_json}

            Your goal is to create a *brief*, one-sentence summary of your immediate plan.
            What is the *first logical step* you will take to investigate this?
            Example: 'My first step is to analyze the event and check the user's payment history.'
            """

            logging.info("Sending prompt to Llama 3...")

            # 2. Call Llama 3 (Ollama)
            response = self.ollama_client.chat(
                model='llama3',
                messages=[{'role': 'user', 'content': prompt}]
            )

            plan = response['message']['content'].strip()
            logging.info(f"LLM (Llama 3) Plan: {plan}")

            # 3. Send the plan back to the Java service
            return agent_pb2.IncidentResponse(status="ACKNOWLEDGED", agent_response=plan)

        except Exception as e:
            error_msg = f"Error during LLM processing: {e}"
            logging.error(error_msg)
            return agent_pb2.IncidentResponse(status="ERROR", agent_response=error_msg)


def serve():
    """Starts the gRPC server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    agent_pb2_grpc.add_AgentServiceServicer_to_server(AgentService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    logging.info("Python Agent Host (gRPC) started on port 50051.")

    try:
        while True:
            time.sleep(86400)  # One day in seconds
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()
