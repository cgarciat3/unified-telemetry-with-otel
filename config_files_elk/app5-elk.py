import os
import time
import random
import logging
import sqlite3
import math
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

# OpenTelemetry Imports
import elasticapm
from elasticapm.contrib.starlette import make_apm_client, ElasticAPM
import ecs_logging


# --- Configuration ---
SERVICE_NAME = os.getenv("ELASTIC_APM_SERVICE_NAME", "unified-demo-elk")
APM_SERVER_URL = os.getenv("ELASTIC_APM_SERVER_URL", "http://192.168.1.134:8200")
DB_PATH = os.getenv("DB_PATH", "./demo.db")
LOG_PATH = os.getenv("LOG_PATH", "/var/log/demo_api")
LOG_FILE = os.path.join(LOG_PATH, "app.log")

apm_config = {
    'SERVICE_NAME': SERVICE_NAME,
    'SERVER_URL': APM_SERVER_URL,
    'ENVIRONMENT': 'production',
    'CAPTURE_BODY': 'all',
    'TRANSACTION_IGNORE_URLS': ['/healthcheck', '/favicon.ico']
}
apm_client = make_apm_client(apm_config)

# Logs Setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
#handler = logging.StreamHandler()
handler = logging.FileHandler(LOG_FILE)
handler.setFormatter(ecs_logging.StdlibFormatter())
logger.addHandler(handler)

# Application Setup
app = FastAPI()
app.add_middleware(ElasticAPM, client=apm_client)

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY, amount REAL, currency TEXT, status TEXT)")
    return conn

class TransactionModel(BaseModel):
    amount: float
    currency: str = "EUR"

@app.post("/process_transaction")
def process_transaction(txn: TransactionModel, fail_rate: float = 0.3):
    """
    Function to simulate business transaction.
    It simulates operations and inserts into db, recording status.
    It doesn't support custom business logic metrics.
    """
    
    with elasticapm.capture_span("process_payment"):
        elasticapm.label(transaction_currency=txn.currency, transaction_amount=txn.amount)
        
        logger.info(f"Starting transaction processing for {txn.amount} {txn.currency}")

        with elasticapm.capture_span("fraud_check"):
            start = time.perf_counter()
            val = 0
            for i in range(500_000):
                val += math.sqrt(i)
            logger.info("Fraud check passed successfully")

        try:
            conn = get_conn()
            cur = conn.cursor()
            if random.random() < fail_rate:
                cur.execute("INSERT INTO fake_db (amount, currency, status) VALUES (?, ?, ?)", 
                            (txn.amount, txn.currency, "SUCCESS"))
            else:
                cur.execute("INSERT INTO transactions (amount, currency, status) VALUES (?, ?, ?)", 
                            (txn.amount, txn.currency, "SUCCESS"))
            conn.commit()
            conn.close()
            
            elasticapm.label(business_status="success")
            elasticapm.set_transaction_result("SUCCESS")
            
        except Exception as e:
            apm_client.capture_exception()
            elasticapm.label(business_status="failed")
            elasticapm.set_transaction_result("FAILURE")
            
            logger.error("Database failed to save transaction", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Transaction failed. trace_id: {elasticapm.get_trace_id()}")

        return {"status": "processed", "trace_id": elasticapm.get_trace_id()}

@app.get("/maintenance")
def maintenance_task(mult: int = 5):
    """
    This funciton simulates cpu/memory load for hostmetrics correlation.
    """
    with elasticapm.capture_span("system_maintenance"):
        elasticapm.label(maintenance_intensity=mult)
        logger.warning("Starting high-intensity maintenance task.")

        start = time.perf_counter()
        
        data = [random.random() for _ in range(mult * 100_000)]
        data.sort()
        
        duration = (time.perf_counter() - start) * 1000
        elasticapm.label(task_type="sort", processing_duration_ms=duration)
        
        logger.info(f"Maintenance finished in {duration:.2f}ms")
        return {"status": "done", "duration_ms": duration}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)