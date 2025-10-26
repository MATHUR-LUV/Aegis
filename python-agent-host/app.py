import grpc
import ollama
import requests # New import
from concurrent import futures
import time
import logging
import os
import json # New import

# Import the generated gRPC files from the 'generated' sub-directory
from generated import agent_pb2
from generated import agent_pb2_grpc

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Get the host for Ollama (defaults to host.docker.internal)
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://host.docker.internal:11434")
# Define the URL for our payment tool service
PAYMENT_TOOL_URL = "http://payment-tool:8000" # Use the service name from docker-compose

# --- Tool Definitions ---
# This structure describes the available tools to the LLM
AVAILABLE_TOOLS = [
    {
        "name": "get_payment_methods",
        "description": "Retrieves the list of saved payment methods for a specific user.",
        "parameters": {
            "type": "object",
            "properties": {
                "user_id": {
                    "type": "integer",
                    "description": "The unique identifier of the user."
                }
            },
            "required": ["user_id"]
        }
    },
    {
        "name": "retry_payment",
        "description": "Attempts to retry a failed payment using a specified payment method ID.",
        "parameters": {
            "type": "object",
            "properties": {
                "order_id": {
                    "type": "integer",
                    "description": "The unique identifier of the order."
                },
                 "payment_method_id": {
                    "type": "string",
                    "description": "The ID of the payment method to use for the retry."
                }
            },
            "required": ["order_id", "payment_method_id"]
        }
    }
    # We will add RAG and escalation tools later
]

class AgentService(agent_pb2_grpc.AgentServiceServicer):
    """Implements the gRPC AgentService."""

    def __init__(self):
        self.ollama_client = None
        self.connect_to_ollama()

    def connect_to_ollama(self):
        """Connects to the Ollama client."""
        try:
            log.info(f"Connecting to Ollama at {OLLAMA_HOST}...")
            self.ollama_client = ollama.Client(host=OLLAMA_HOST)
            self.ollama_client.list() # Check connection
            log.info("Ollama client initialized and ready.")
        except Exception as e:
            log.error(f"Failed to connect to Ollama: {e}")
            log.error("Please ensure Ollama is running on your host machine")
            self.ollama_client = None

    # --- Tool Calling Function ---
    def call_payment_tool(self, tool_name, tool_args):
        """Calls the payment tool microservice."""
        api_endpoint = ""
        if tool_name == "get_payment_methods":
            api_endpoint = f"{PAYMENT_TOOL_URL}/get_payment_methods"
            log.info(f"Calling Payment Tool: POST {api_endpoint} with args: {tool_args}")
        elif tool_name == "retry_payment":
            api_endpoint = f"{PAYMENT_TOOL_URL}/retry_payment"
            log.info(f"Calling Payment Tool: POST {api_endpoint} with args: {tool_args}")
        else:
             log.warning(f"Attempted to call unknown tool: {tool_name}")
             return {"error": f"Tool '{tool_name}' not recognized."}

        # Make the actual HTTP request
        try:
            response = requests.post(api_endpoint, json=tool_args)
            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
            tool_result = response.json()
            log.info(f"Payment Tool Response: {tool_result}")
            return tool_result
        except requests.exceptions.RequestException as e:
            log.error(f"Error calling Payment Tool ({tool_name}): {e}")
            return {"error": f"Failed to call tool {tool_name}: {e}"}


    def HandleIncident(self, request, context):
        """Handles the incoming gRPC request (potentially multiple turns)."""
        log.info(f"Received incident from Java: {request.event_type} - {request.full_event_json}")
        final_summary = "Agent processing failed unexpectedly." # Default fail message

        if not self.ollama_client:
            error_msg = "Ollama client not initialized. Cannot process incident."
            log.error(error_msg)
            return agent_pb2.IncidentResponse(status="ERROR", agent_response=error_msg)

        try:
            # --- Conversation History ---
            # We'll build up the messages for the LLM turn by turn
            conversation_history = []

            # --- Turn 1: Initial Planning & First Tool Call ---
            try:
                 event_data = json.loads(request.full_event_json)
                 user_id_from_event = event_data.get('user_id')
                 order_id_from_event = event_data.get('order_id') # Get order_id too
            except json.JSONDecodeError:
                 user_id_from_event = None
                 order_id_from_event = None
                 log.warning("Could not parse event JSON for IDs.")


            initial_prompt = f"""
            You are 'Aegis', an autonomous incident resolution agent.
            You have received a critical incident:
            - Event Type: {request.event_type}
            - Full Event JSON: {request.full_event_json}

            Available tools: {json.dumps(AVAILABLE_TOOLS, indent=2)}

            Your goal is to resolve this payment failure.
            1. What is the *first* tool you need to call to investigate?
            2. Respond ONLY with a JSON object containing the 'tool_name' and 'tool_args' for that first call. Use user_id {user_id_from_event} if available.

            Example Response Format:
            {{
              "tool_name": "get_payment_methods",
              "tool_args": {{ "user_id": 123 }}
            }}
            """
            conversation_history.append({'role': 'user', 'content': initial_prompt})

            log.info("Sending initial prompt to Llama 3 for planning...")
            response = self.ollama_client.chat(
                model='llama3',
                messages=conversation_history,
                format='json' # Ask Ollama for JSON output
            )
            llm_decision_str = response['message']['content'].strip()
            log.info(f"LLM (Llama 3) Turn 1 Plan (JSON): {llm_decision_str}")
            conversation_history.append({'role': 'assistant', 'content': llm_decision_str}) # Add LLM response to history

            # --- Execute the First Tool Call ---
            try:
                llm_decision = json.loads(llm_decision_str)
                tool_name = llm_decision.get("tool_name")
                tool_args = llm_decision.get("tool_args")

                if tool_name and tool_args:
                    # Ensure user_id is correct integer
                    if tool_name == "get_payment_methods":
                         if 'user_id' not in tool_args and user_id_from_event:
                              log.warning(f"LLM tool args missing user_id, using ID from event: {user_id_from_event}")
                              tool_args['user_id'] = user_id_from_event
                         if 'user_id' in tool_args:
                             try:
                                 tool_args['user_id'] = int(tool_args['user_id'])
                             except (ValueError, TypeError):
                                  raise ValueError(f"LLM provided non-integer user_id: {tool_args['user_id']}")

                    tool_result = self.call_payment_tool(tool_name, tool_args)
                    tool_result_str = json.dumps(tool_result)
                    conversation_history.append({'role': 'tool', 'content': tool_result_str}) # Add tool result
                else:
                    raise ValueError("LLM did not provide a valid tool name and arguments.")

            except (json.JSONDecodeError, ValueError) as e:
                log.error(f"Error processing LLM Turn 1 or executing tool: {e}")
                return agent_pb2.IncidentResponse(status="ERROR", agent_response=f"Error after Turn 1: {e}")

            # --- Turn 2: Analyze Tool Result & Plan/Execute Second Tool Call ---
            second_prompt = f"""
            Analyze the result from the '{tool_name}' call: {tool_result_str}
            Available tools: {json.dumps(AVAILABLE_TOOLS, indent=2)}

            Decide the *next* step to resolve the payment failure for order_id {order_id_from_event}:
            - If you found an 'active' payment method (e.g., 'card_B_paypal'), your next step is to call the 'retry_payment' tool. Respond ONLY with the JSON for that tool call, using the correct order_id and payment_method_id.
            - If there are no 'active' methods or the first tool call failed, provide a final summary message that escalation is needed. Do NOT respond with JSON in this case.
            """
            conversation_history.append({'role': 'user', 'content': second_prompt})

            log.info("Sending prompt to Llama 3 for Turn 2 decision...")
            response = self.ollama_client.chat(
                model='llama3',
                messages=conversation_history, # Send full history
                format='json' # Ask for JSON again (might be another tool call)
            )
            llm_turn2_response_str = response['message']['content'].strip()
            log.info(f"LLM (Llama 3) Turn 2 Decision/Summary: {llm_turn2_response_str}")
            conversation_history.append({'role': 'assistant', 'content': llm_turn2_response_str}) # Add to history

            # --- Check if LLM planned another tool call ---
            try:
                llm_turn2_decision = json.loads(llm_turn2_response_str)
                tool_name_2 = llm_turn2_decision.get("tool_name")
                tool_args_2 = llm_turn2_decision.get("tool_args")

                if tool_name_2 == "retry_payment" and tool_args_2:
                    log.info(f"LLM plans to call '{tool_name_2}'. Executing...")
                    # --- Execute the Second Tool Call ---
                     # Ensure order_id is correct integer
                    if 'order_id' not in tool_args_2 and order_id_from_event:
                         log.warning(f"LLM tool args missing order_id, using ID from event: {order_id_from_event}")
                         tool_args_2['order_id'] = order_id_from_event
                    if 'order_id' in tool_args_2:
                         try:
                             tool_args_2['order_id'] = int(tool_args_2['order_id'])
                         except (ValueError, TypeError):
                              raise ValueError(f"LLM provided non-integer order_id: {tool_args_2['order_id']}")

                    tool_result_2 = self.call_payment_tool(tool_name_2, tool_args_2)
                    tool_result_2_str = json.dumps(tool_result_2)
                    conversation_history.append({'role': 'tool', 'content': tool_result_2_str}) # Add result

                    # --- Turn 3: Get Final Summary ---
                    third_prompt = f"""
                    You just attempted to retry the payment using '{tool_name_2}' with args {tool_args_2}.
                    The result was: {tool_result_2_str}

                    Provide a final, concise summary message for the system log based on this result (e.g., "Payment retry successful.", "Payment retry failed: Insufficient funds."). Do NOT respond with JSON.
                    """
                    conversation_history.append({'role': 'user', 'content': third_prompt})

                    log.info("Sending prompt to Llama 3 for final summary...")
                    response = self.ollama_client.chat(
                        model='llama3',
                        messages=conversation_history # Send full history
                        # format='text' implied default
                    )
                    final_summary = response['message']['content'].strip()
                    log.info(f"LLM (Llama 3) Final Summary: {final_summary}")

                else:
                    # LLM decided on something other than retry_payment in Turn 2
                    log.warning(f"LLM Turn 2 response was JSON but not a retry_payment call: {llm_turn2_response_str}")
                    final_summary = "Agent decided not to retry payment based on available methods." # Default summary

            except json.JSONDecodeError:
                 # If LLM Turn 2 response wasn't JSON, assume it's the final summary (e.g., escalation needed)
                 log.info("LLM Turn 2 response was text, assuming final summary.")
                 final_summary = llm_turn2_response_str
            except ValueError as e: # Catch tool arg errors
                 log.error(f"Error processing LLM Turn 2 or executing tool: {e}")
                 final_summary = f"Error processing Turn 2 decision: {e}"


            # --- Return the final outcome ---
            log.info(f"Returning final summary to Java: {final_summary}")
            return agent_pb2.IncidentResponse(status="COMPLETED", agent_response=final_summary) # Use COMPLETED status

        except Exception as e:
            error_msg = f"Unhandled error in HandleIncident: {e}"
            log.error(error_msg, exc_info=True) # Log full traceback
            return agent_pb2.IncidentResponse(status="ERROR", agent_response=error_msg)


def serve():
    """Starts the gRPC server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    agent_pb2_grpc.add_AgentServiceServicer_to_server(AgentService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    log.info("Python Agent Host (gRPC) started on port 50051.")
    try:
        while True:
            time.sleep(86400) # One day
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()

