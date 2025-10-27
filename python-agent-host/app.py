import grpc
import ollama
import requests
from concurrent import futures
import time
import logging
import os
import json
from elasticsearch import Elasticsearch, exceptions # New import

# Import the generated gRPC files from the 'generated' sub-directory
from generated import agent_pb2
from generated import agent_pb2_grpc

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# --- Configuration ---
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://host.docker.internal:11434")
PAYMENT_TOOL_URL = "http://payment-tool:8000"
ELASTICSEARCH_HOST = "http://elasticsearch:9200"
ELASTICSEARCH_INDEX = "aegis_policies"

# --- Tool Definitions (Updated) ---
# ... (Keep AVAILABLE_TOOLS exactly the same as before, including all 4 tools) ...
AVAILABLE_TOOLS = [
    {
        "name": "get_payment_methods",
        "description": "Retrieves the list of saved payment methods for a specific user.",
        "parameters": {
            "type": "object",
            "properties": { "user_id": { "type": "integer", "description": "The unique identifier of the user."}},
            "required": ["user_id"]
        }
    },
    {
        "name": "retry_payment",
        "description": "Attempts to retry a failed payment using a specified payment method ID.",
        "parameters": {
             "type": "object",
             "properties": { "order_id": {"type": "integer"}, "payment_method_id": {"type": "string"}},
             "required": ["order_id", "payment_method_id"]
        }
    },
    {
        "name": "query_knowledge_base",
        "description": "Searches the company policy knowledge base for relevant procedures.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The question or topic to search for in the policy documents (e.g., 'policy for multiple payment failures')."
                }
            },
            "required": ["query"]
        }
    },
     {
        "name": "escalate_to_human",
        "description": "Creates a ticket for human review when automatic resolution fails.",
        "parameters": {
            "type": "object",
            "properties": {
                "order_id": {"type": "integer", "description": "The order ID needing review."},
                "reason": {"type": "string", "description": "A brief reason for escalation."}
            },
            "required": ["order_id", "reason"]
        }
    }
]


class AgentService(agent_pb2_grpc.AgentServiceServicer):
    """Implements the gRPC AgentService."""

    def __init__(self):
        self.ollama_client = None
        self.es_client = None
        self.connect_to_ollama()
        self.connect_to_elasticsearch()

    def connect_to_ollama(self):
        # ... (same as before) ...
        try:
            log.info(f"Connecting to Ollama at {OLLAMA_HOST}...")
            self.ollama_client = ollama.Client(host=OLLAMA_HOST)
            self.ollama_client.list() # Check connection
            log.info("Ollama client initialized and ready.")
        except Exception as e:
            log.error(f"Failed to connect to Ollama: {e}")
            self.ollama_client = None


    def connect_to_elasticsearch(self):
        """Connects to the Elasticsearch service with retries."""
        log.info(f"Attempting to connect to Elasticsearch at {ELASTICSEARCH_HOST}...")
        # Use the service name defined in docker-compose
        self.es_client = Elasticsearch(
            ELASTICSEARCH_HOST,
            request_timeout=10 # Add a timeout
        )
        retries = 5
        while retries > 0:
            try:
                # Use client.info() which is more reliable than ping() across versions
                if self.es_client.info():
                    log.info("Successfully connected to Elasticsearch.")
                    return # Success
                else:
                     log.warning("Elasticsearch info() returned False, retrying...")
            except exceptions.ConnectionTimeout as e:
                 log.warning(f"Elasticsearch connection timed out ({e}), retrying...")
            except exceptions.ConnectionError as e:
                log.warning(f"Elasticsearch connection error ({type(e).__name__}), retrying...")
            except Exception as e:
                log.warning(f"Unexpected error connecting to Elasticsearch ({type(e).__name__}), retrying...")

            retries -= 1
            time.sleep(5) # Wait 5 seconds before retrying

        log.error("Failed to connect to Elasticsearch after several retries. RAG will not function.")
        self.es_client = None # Ensure client is None if connection failed


    # --- Tool Calling Functions ---
    def call_payment_tool(self, tool_name, tool_args):
        # ... (same as before, handles get_payment_methods and retry_payment) ...
        api_endpoint = ""
        if tool_name == "get_payment_methods":
            api_endpoint = f"{PAYMENT_TOOL_URL}/get_payment_methods"
            log.info(f"Calling Payment Tool: POST {api_endpoint} with args: {tool_args}")
        elif tool_name == "retry_payment":
            api_endpoint = f"{PAYMENT_TOOL_URL}/retry_payment"
            log.info(f"Calling Payment Tool: POST {api_endpoint} with args: {tool_args}")
        else:
             log.warning(f"Attempted to call unknown payment tool: {tool_name}")
             return {"error": f"Tool '{tool_name}' not recognized by payment tool caller."}
        try:
            response = requests.post(api_endpoint, json=tool_args)
            response.raise_for_status()
            tool_result = response.json()
            log.info(f"Payment Tool Response: {tool_result}")
            return tool_result
        except requests.exceptions.RequestException as e:
            log.error(f"Error calling Payment Tool ({tool_name}): {e}")
            return {"error": f"Failed to call tool {tool_name}: {e}"}


    def call_knowledge_base(self, query):
        # ... (same as before) ...
        if not self.es_client:
            log.error("Cannot query knowledge base: Elasticsearch client not connected.")
            return {"error": "Knowledge base (Elasticsearch) is unavailable."}

        log.info(f"Querying Knowledge Base (Elasticsearch) for: '{query}'")
        try:
            response = self.es_client.search(
                index=ELASTICSEARCH_INDEX,
                query={"match": {"content": query}}
            )
            hits = response['hits']['hits']
            if hits:
                best_hit_content = hits[0]['_source']['content']
                log.info(f"Found relevant policy: {hits[0]['_source']['policy_id']}")
                # Return slightly more structured data
                return {"policy_found": True, "policy_id": hits[0]['_source']['policy_id'], "policy_content": best_hit_content}
            else:
                log.info("No relevant policies found in knowledge base.")
                return {"policy_found": False, "policy_content": "No relevant policy found."}
        except exceptions.ElasticsearchException as e:
            log.error(f"Error querying Elasticsearch: {e}")
            return {"error": f"Error querying knowledge base: {e}"}
        except Exception as e:
             log.error(f"Unexpected error during knowledge base query: {e}")
             return {"error": f"Unexpected error querying knowledge base: {e}"}


    def call_escalate_to_human(self, order_id, reason):
        # ... (same as before) ...
        log.warn(f"ESCALATION TRIGGERED for order {order_id}. Reason: {reason}. Ticket #SIM{order_id} created.")
        return {"ticket_id": f"SIM{order_id}", "status": "Escalation ticket created successfully."}


    def HandleIncident(self, request, context):
        """Handles the incoming gRPC request (multi-turn with RAG)."""
        log.info(f"Received incident: {request.event_type} - {request.full_event_json}")
        final_summary = "Agent processing completed with errors." # Default

        # --- Connection Checks ---
        if not self.ollama_client:
            error_msg = "Ollama client not initialized."
            log.error(error_msg)
            return agent_pb2.IncidentResponse(status="ERROR", agent_response=error_msg)
        # We will handle RAG call failure later if es_client is None

        try:
            # --- Setup ---
            conversation_history = []
            try:
                 event_data = json.loads(request.full_event_json)
                 user_id_from_event = event_data.get('user_id')
                 order_id_from_event = event_data.get('order_id')
            except json.JSONDecodeError:
                 user_id_from_event = None
                 order_id_from_event = None
                 log.warning("Could not parse event JSON for IDs.")

            current_turn = 1
            max_turns = 5
            last_tool_name_planned_in_prev_turn = None # Track the tool planned in the PREVIOUS turn

            # --- Main Agent Loop ---
            while current_turn <= max_turns:
                log.info(f"--- Agent Turn {current_turn} ---")
                log.info(f"Start of Turn {current_turn}. Last tool planned: {last_tool_name_planned_in_prev_turn}")
                prompt = ""
                # --- **REVISED EXPECTATION LOGIC** ---
                # Default: Expect JSON unless it's explicitly the final summary turn
                expect_json = True
                if last_tool_name_planned_in_prev_turn == "retry_payment":
                    # After retry, could be JSON (RAG call) or Text (Success summary)
                    expect_json = None
                elif last_tool_name_planned_in_prev_turn == "escalate_to_human":
                    # After escalation, must be Text (Final summary)
                    expect_json = False
                # --- **END REVISED EXPECTATION LOGIC** ---


                # --- Construct Prompt Based on Turn and PREVIOUSLY PLANNED tool ---
                # Turn 1: Always get payment methods first
                if current_turn == 1:
                    prompt = f"""
                    You are 'Aegis', resolving a '{request.event_type}' incident for order_id {order_id_from_event}, user_id {user_id_from_event}.
                    Event Details: {request.full_event_json}
                    Available tools: {json.dumps(AVAILABLE_TOOLS, indent=2)}
                    Goal: Resolve this payment failure.
                    Your *very first step* MUST be to call 'get_payment_methods'.
                    Respond ONLY with a valid JSON object containing exactly two keys: "tool_name" (string) and "tool_args" (object).
                    The value for "tool_name" must be "get_payment_methods".
                    The value for "tool_args" must be an object containing the key "user_id" with the value {user_id_from_event}.
                    DO NOT include descriptions, parameters, or any other keys in your JSON response.
                    """
                # Turn 2: Should follow 'get_payment_methods'
                elif last_tool_name_planned_in_prev_turn == "get_payment_methods":
                    last_tool_result = conversation_history[-1]['content'] if len(conversation_history) > 0 and conversation_history[-1]['role'] == 'tool' else '{"error": "Could not get previous tool result"}'
                    prompt = f"""
                    Result of 'get_payment_methods': {last_tool_result}
                    Available tools: {json.dumps(AVAILABLE_TOOLS, indent=2)}
                    Analyze the result.
                    - If you see an 'active' payment method, your next step MUST be to call 'retry_payment'. Respond ONLY with JSON containing "tool_name": "retry_payment" and "tool_args" object including the correct order_id ({order_id_from_event}) and the active payment_method_id.
                    - If there are NO 'active' methods, your next step MUST be to call 'query_knowledge_base' about the policy for 'no active backup methods'. Respond ONLY with JSON containing "tool_name": "query_knowledge_base" and "tool_args" object with the query.
                    """
                # Turn 3: Should follow 'retry_payment'
                elif last_tool_name_planned_in_prev_turn == "retry_payment":
                    last_tool_result = conversation_history[-1]['content'] if len(conversation_history) > 0 and conversation_history[-1]['role'] == 'tool' else '{"error": "Could not get previous tool result"}'
                    prompt = f"""
                    Result of 'retry_payment': {last_tool_result}
                    Available tools: {json.dumps(AVAILABLE_TOOLS, indent=2)}
                    Analyze the result.
                    - If the status is 'success', provide ONLY the final summary text (NO JSON).
                    - If the status is 'failed', your next step MUST be to call 'query_knowledge_base' about the policy for 'multiple payment failures'. Respond ONLY with JSON containing "tool_name": "query_knowledge_base" and "tool_args" object with the query.
                    """
                # Turn 4: Should follow 'query_knowledge_base'
                elif last_tool_name_planned_in_prev_turn == "query_knowledge_base":
                     last_tool_result = conversation_history[-1]['content'] if len(conversation_history) > 0 and conversation_history[-1]['role'] == 'tool' else '{"error": "Could not get previous tool result"}'
                     prompt = f"""
                     Result of 'query_knowledge_base': {last_tool_result}
                     Available tools: {json.dumps(AVAILABLE_TOOLS, indent=2)}
                     Analyze the policy content found. Your next step MUST be to call the 'escalate_to_human' tool.
                     Respond ONLY with JSON containing "tool_name": "escalate_to_human" and "tool_args" object including the order_id ({order_id_from_event}) and a brief reason based on the policy.
                     """
                # Turn 5: Should follow 'escalate_to_human' (final summary)
                elif last_tool_name_planned_in_prev_turn == "escalate_to_human":
                     last_tool_result = conversation_history[-1]['content'] if len(conversation_history) > 0 and conversation_history[-1]['role'] == 'tool' else '{"error": "Could not get previous tool result"}'
                     prompt = f"""
                     Result of 'escalate_to_human': {last_tool_result}
                     Provide ONLY the final, concise summary message based on the escalation result (NO JSON).
                     """
                else:
                    log.error(f"Agent reached unexpected state at Turn {current_turn}. Last tool planned: {last_tool_name_planned_in_prev_turn}. History: {conversation_history}")
                    final_summary = "Agent reached an unexpected state."
                    break # Exit loop

                conversation_history.append({'role': 'user', 'content': prompt})

                # --- Call LLM ---
                log.info(f"Sending prompt to Llama 3 (Turn {current_turn}). Expect JSON: {expect_json}")
                llm_format = 'json' if expect_json is True else None
                try:
                    response = self.ollama_client.chat(
                        model='llama3',
                        messages=conversation_history,
                        format=llm_format
                    )
                    llm_response_str = response['message']['content'].strip()
                except Exception as llm_error:
                    log.error(f"Error calling Ollama: {llm_error}")
                    final_summary = f"Error communicating with LLM: {llm_error}"
                    break # Exit loop on LLM error

                log.info(f"LLM (Llama 3) Turn {current_turn} Response: {llm_response_str}")
                conversation_history.append({'role': 'assistant', 'content': llm_response_str})

                # --- Process LLM Response ---
                # Reset planned tool for the next iteration before processing
                last_tool_name_planned_in_prev_turn = None
                tool_executed_this_turn = False
                try:
                    # Always TRY to parse as JSON first, even if expect_json is None or False
                    llm_decision = json.loads(llm_response_str)
                    tool_name = llm_decision.get("tool_name")
                    tool_args = llm_decision.get("tool_args")

                    # Check if it LOOKS like a valid tool call structure
                    if tool_name and isinstance(tool_args, dict):
                         # --- **REVISED LOGIC: If it looks like a tool call, execute it.** ---
                         log.info(f"LLM response parsed as JSON tool call for '{tool_name}'.")
                         last_tool_name_planned_in_prev_turn = tool_name # Record for next turn

                         # --- Execute Tool ---
                         log.info(f"Executing tool call: {tool_name} with args: {tool_args}")
                         tool_result = {}
                         # (Argument handling needs refinement)
                         if tool_name == "get_payment_methods":
                             if 'user_id' not in tool_args and user_id_from_event: tool_args['user_id'] = user_id_from_event
                             tool_result = self.call_payment_tool(tool_name, tool_args)
                         elif tool_name == "retry_payment":
                             if 'order_id' not in tool_args and order_id_from_event: tool_args['order_id'] = order_id_from_event
                             tool_result = self.call_payment_tool(tool_name, tool_args)
                         elif tool_name == "query_knowledge_base":
                              if not self.es_client:
                                   tool_result = {"error": "Knowledge base (Elasticsearch) is unavailable."}
                                   log.error(tool_result["error"])
                              else:
                                  tool_result = self.call_knowledge_base(tool_args.get('query', ''))
                         elif tool_name == "escalate_to_human":
                             if 'order_id' not in tool_args and order_id_from_event: tool_args['order_id'] = order_id_from_event
                             tool_result = self.call_escalate_to_human(tool_args.get('order_id'), tool_args.get('reason', 'Reason not specified'))
                             # Escalation result needs to be summarized in the next turn
                         else:
                             tool_result = {"error": f"Tool '{tool_name}' execution not implemented."}
                             log.warning(tool_result["error"])

                         # Add tool result to history
                         tool_result_str = json.dumps(tool_result)
                         conversation_history.append({'role': 'tool', 'content': tool_result_str})
                         tool_executed_this_turn = True # Mark that we executed a tool

                    else: # Parsed as JSON, but doesn't look like a tool call
                        log.info("LLM response was JSON but not a valid tool call format, assuming final summary.")
                        final_summary = llm_response_str
                        break # Exit loop

                except json.JSONDecodeError:
                    # It wasn't JSON. If we expected JSON, it's an error. Otherwise, it's the summary.
                    if expect_json is True:
                         log.error(f"Expected JSON tool call, but received text: {llm_response_str}")
                         final_summary = "LLM failed to provide expected JSON tool call."
                         break # Exit loop on error
                    else: # Expected text or either (None), so treat as final summary
                         log.info("LLM response was text as expected or allowed, assuming final summary.")
                         final_summary = llm_response_str
                         break # Exit loop

                except ValueError as e: # Catch argument processing errors etc.
                     log.error(f"Error processing LLM response or tool args: {e}. Raw response: {llm_response_str}")
                     final_summary = f"Error processing LLM response: {e}"
                     break # Exit loop on error

                # Check if we should move to the next turn
                if not tool_executed_this_turn:
                     log.error("Loop reached end without executing a tool or reaching a final summary. Breaking.")
                     final_summary = "Agent loop error."
                     break

                current_turn += 1 # Move to the next turn

            # --- End of Loop ---
            if current_turn > max_turns:
                final_summary = "Agent reached max turns without full resolution."
                log.warning(final_summary)
                return agent_pb2.IncidentResponse(status="ERROR", agent_response=final_summary)
            else: # Loop exited via 'break'
                log.info(f"Returning final summary to Java: {final_summary}")
                return agent_pb2.IncidentResponse(status="COMPLETED", agent_response=final_summary)

        except Exception as e:
            error_msg = f"Unhandled error in HandleIncident: {e}"
            log.error(error_msg, exc_info=True)
            return agent_pb2.IncidentResponse(status="ERROR", agent_response=error_msg)

# ... (Keep the serve() function exactly the same) ...
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