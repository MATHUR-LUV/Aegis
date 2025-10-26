import os
import psycopg2
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import logging

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

app = FastAPI()

# --- Database Connection ---
def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            port=os.environ.get("DB_PORT"),
            database=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD")
        )
        log.info("Database connection successful")
        return conn
    except Exception as e:
        log.error(f"Database connection failed: {e}")
        return None


# --- Pydantic Models for Request/Response Validation ---
class PaymentMethodRequest(BaseModel):
    user_id: int


class PaymentMethod(BaseModel):
    payment_method_id: str
    method_type: str
    status: str


class PaymentMethodResponse(BaseModel):
    payment_methods: list[PaymentMethod]


# --- NEW: Models for Retry Payment ---
class RetryPaymentRequest(BaseModel):
    order_id: int
    payment_method_id: str


class RetryPaymentResponse(BaseModel):
    status: str  # "success" or "failed"
    transaction_id: str | None = None  # Optional transaction ID on success
    reason: str | None = None  # Optional failure reason


# --- API Endpoints ---
@app.get("/health")
async def health_check():
    """Basic health check endpoint."""
    return {"status": "ok"}


@app.post("/get_payment_methods", response_model=PaymentMethodResponse)
async def get_payment_methods(request: PaymentMethodRequest):
    """Fetches payment methods for a given user_id from the database."""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=503, detail="Database connection unavailable")

    methods = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payment_method_id, method_type, status FROM payment_methods WHERE user_id = %s",
                (request.user_id,)
            )
            rows = cur.fetchall()
            for row in rows:
                methods.append(PaymentMethod(payment_method_id=row[0], method_type=row[1], status=row[2]))
        log.info(f"Fetched {len(methods)} payment methods for user {request.user_id}")
    except Exception as e:
        log.error(f"Error fetching payment methods: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from database")
    finally:
        if conn:
            conn.close()

    return PaymentMethodResponse(payment_methods=methods)


# --- NEW: Retry Payment Endpoint ---
@app.post("/retry_payment", response_model=RetryPaymentResponse)
async def retry_payment(request: RetryPaymentRequest):
    """
    MOCK endpoint to simulate retrying a payment.
    - 'card_B_paypal' will succeed.
    - Any other method ID will fail.
    """
    log.info(f"Received retry payment request for order {request.order_id} using method {request.payment_method_id}")

    # Simple mock logic
    if request.payment_method_id == "card_B_paypal":
        log.info("Simulating SUCCESSFUL payment retry for PayPal.")
        return RetryPaymentResponse(status="success", transaction_id=f"txn_{request.order_id}_paypal")
    else:
        log.warning(f"Simulating FAILED payment retry for method {request.payment_method_id}.")
        return RetryPaymentResponse(status="failed", reason="Insufficient funds (mocked)")


