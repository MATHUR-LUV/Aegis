import os
import time
from elasticsearch import Elasticsearch, exceptions
import requests # Import requests

# Configuration
ELASTICSEARCH_HOST_URL = "http://127.0.0.1:9200" # Use IP address
INDEX_NAME = "aegis_policies"
POLICY_DIR = "./policies"

# --- NEW: Basic Requests Test ---
def test_connection_with_requests():
    print(f"Attempting basic connection to {ELASTICSEARCH_HOST_URL} using requests...")
    try:
        response = requests.get(ELASTICSEARCH_HOST_URL, timeout=5) # 5 second timeout
        response.raise_for_status() # Raise exception for bad status codes
        print(f"Requests connection successful! Status code: {response.status_code}")
        # print(f"Response JSON: {response.json()}") # Optionally print the JSON
        return True
    except requests.exceptions.Timeout:
        print("Requests connection timed out.")
        return False
    except requests.exceptions.ConnectionError as e:
        print(f"Requests connection failed: {e}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"Requests encountered an error: {e}")
        return False
# --- END NEW ---


def create_es_client():
    """Creates and waits for Elasticsearch client connection using client.info()."""
    # Use the same URL as the requests test
    es_client = Elasticsearch(ELASTICSEARCH_HOST_URL)
    retries = 3 # Reduce retries for faster testing
    while retries > 0:
        try:
            # Try getting basic cluster info instead of ping
            info = es_client.info()
            print("Elasticsearch library connection successful.")
            print(f"Cluster Name: {info.get('cluster_name')}") # Print some info
            return es_client
        except exceptions.ConnectionError as e:
            print(f"Elasticsearch library connection error ({type(e).__name__}), retrying...")
        except Exception as e:
            # Catch other potential errors during info() call
            print(f"Elasticsearch library unexpected error connecting ({type(e).__name__}), retrying...")

        retries -= 1
        time.sleep(3) # Wait 3 seconds before retrying

    print("Failed to connect using Elasticsearch library after several retries.")
    return None

def index_policies(es_client):
    """Reads policy files and indexes them into Elasticsearch."""
    # (Keep this function exactly as it was before)
    if es_client.indices.exists(index=INDEX_NAME):
        print(f"Index '{INDEX_NAME}' already exists. Skipping indexing.")
        return

    print(f"Creating index '{INDEX_NAME}'...")
    es_client.indices.create(index=INDEX_NAME)

    doc_id_counter = 1
    for filename in os.listdir(POLICY_DIR):
        if filename.endswith(".txt"):
            filepath = os.path.join(POLICY_DIR, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                    doc = {
                        'policy_id': filename.replace('.txt', ''),
                        'content': content
                    }
                    es_client.index(index=INDEX_NAME, id=doc_id_counter, document=doc)
                    print(f"Indexed document: {filename} with ID: {doc_id_counter}")
                    doc_id_counter += 1
            except Exception as e:
                print(f"Error reading or indexing file {filename}: {e}")


if __name__ == "__main__":
    print("--- Starting Elasticsearch Indexing Script ---")
    # --- Run the basic requests test first ---
    if not test_connection_with_requests():
        print("--- Basic HTTP connection failed. Aborting Elasticsearch client attempt. ---")
    else:
        # If basic connection works, try the elasticsearch library
        print("--- Basic HTTP connection succeeded. Now trying Elasticsearch library... ---")
        client = create_es_client()
        if client:
            index_policies(client)
            print("--- Indexing complete (or skipped) ---")
        else:
            print("--- Indexing failed: Could not connect using Elasticsearch library ---")