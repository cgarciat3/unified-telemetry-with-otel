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
from opentelemetry import trace, metrics, _logs
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

# Instrumentation
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlite3 import SQLite3Instrumentor

# --- Configuration ---
SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "unified-demo-api")
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
DB_PATH = os.getenv("DB_PATH", "./demo.db")

resource = Resource.create({"service.name": SERVICE_NAME})

# Traces Setup
trace_provider = TracerProvider(resource=resource)
trace_processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True))
trace_provider.add_span_processor(trace_processor)
trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer(__name__)

# Metrics Setup
metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=OTEL_ENDPOINT, insecure=True),
    export_interval_millis=5000, 
)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter(__name__)

# Logs Setup
logger_provider = LoggerProvider(resource=resource)
_logs.set_logger_provider(logger_provider)
log_exporter = OTLPLogExporter(endpoint=OTEL_ENDPOINT, insecure=True)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))

handler = LoggingHandler(level=logging.INFO, logger_provider=logger_provider)
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)

# Metrics definition
transaction_counter = meter.create_counter(
    name="business.transactions.total",
    unit="1",
    description="Total number of processed transactions"
)

transaction_value = meter.create_histogram(
    name="business.transaction.value",
    unit="EUR",
    description="Value of processed transactions"
)

processing_duration = meter.create_histogram(
    name="app.processing.duration",
    unit="ms",
    description="Time taken for heavy processing tasks"
)

# Application Setup
app = FastAPI()

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
    It also uses custom business logic metrics.
    """

    with tracer.start_as_current_span("process_payment") as span:
        span.set_attribute("transaction.currency", txn.currency)
        span.set_attribute("transaction.amount", txn.amount)
        
        logger.info(f"Starting transaction processing for {txn.amount} {txn.currency}")

        with tracer.start_as_current_span("fraud_check"):
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
                conn.commit()
                conn.close()
            else:
                cur.execute("INSERT INTO transactions (amount, currency, status) VALUES (?, ?, ?)", 
                            (txn.amount, txn.currency, "SUCCESS"))
                conn.commit()
                conn.close()
        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR))
            logger.error("Database failed to save transaction", exc_info=True)
            raise HTTPException(status_code=500, detail="Transaction failed")

        transaction_counter.add(1, {"currency": txn.currency, "status": "success"})
        transaction_value.record(txn.amount, {"currency": txn.currency})

        return {"status": "processed", "trace_id": span.get_span_context().trace_id}

@app.get("/maintenance")
def maintenance_task(mult: int = 5):
    """
    This funciton simulates cpu/memory load for hostmetrics correlation.
    
    """
    
    with tracer.start_as_current_span("system_maintenance") as span:
        span.set_attribute("maintenance.intensity", mult)
        logger.warning(f"Starting high-intensity maintenance task.")

        start = time.perf_counter()
        
        data = [random.random() for _ in range(mult * 100_000)]
        data.sort()
        
        duration = (time.perf_counter() - start) * 1000
        processing_duration.record(duration, {"task_type": "sort"})
        
        logger.info(f"Maintenance finished in {duration:.2f}ms")
        return {"status": "done", "duration_ms": duration}


FastAPIInstrumentor.instrument_app(app)
SQLite3Instrumentor().instrument()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)