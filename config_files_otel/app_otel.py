import time
import random
import logging
import sqlite3
import math
import json
import uuid
from contextvars import ContextVar

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from opentelemetry import  _logs, metrics, trace
from opentelemetry.sdk.resources import Resource
# Logging libraries
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
# Metrics libraries
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import Histogram
from opentelemetry.sdk.metrics.export import AggregationTemporality
# Tracing libraries
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
# Instrumentation Libraries
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlite3 import SQLite3Instrumentor
from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor

# CONFIG
DB_PATH = "./demo.db"

OTEL_ENDPOINT = "http://localhost:4317"
SERVICE_NAME = "app-otel"
resource = Resource.create({"service.name": SERVICE_NAME})

# Logs Setup
logger_provider = LoggerProvider(resource=resource)
_logs.set_logger_provider(logger_provider)
log_exporter = OTLPLogExporter(endpoint=OTEL_ENDPOINT, insecure=True)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))

handler = LoggingHandler(level=logging.INFO, logger_provider=logger_provider)
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)


# Metrics Setup
delta_temporality = {
    Histogram: AggregationTemporality.DELTA
}
metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(
                        endpoint=OTEL_ENDPOINT, 
                        insecure=True, 
                        preferred_temporality=delta_temporality
                    ),
    export_interval_millis=5000, 
)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter(__name__)

# Metrics Definition
database_error_counter = meter.create_counter(
    name="app.database.error.count",
    unit="1",
    description="Total number of errors from database"
)

transaction_amount_sum = meter.create_counter(
    name="business.transactions.sum",
    unit="EUR",
    description="Total of EUR transactioned."
)

processing_duration = meter.create_histogram(
    name="app.processing.duration",
    unit="ms",
    description="Time taken for heavy processing tasks"
)

# Traces Setup
trace_provider = TracerProvider(resource=resource)
span_processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True))
trace_provider.add_span_processor(span_processor)
trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer(__name__)

# App setup
app = FastAPI()
FastAPIInstrumentor.instrument_app(app)
SQLite3Instrumentor().instrument()
SystemMetricsInstrumentor().instrument()

# Logs every request, saving path and execution time
@app.middleware("http")
async def log_requests(request: Request, call_next):
    with tracer.start_as_current_span("entry_function") as span:
        start = time.time()
        logger.info(f"Request to {request.url.path}")
        
        try:
            response = await call_next(request)
            ms = round((time.time() - start) * 1000, 2)
            logger.info("Request OK")
            span.set_attribute("request_result", "Success")
            return response
        except Exception as e:
            ms = round((time.time() - start) * 1000, 2)
            logger.exception(f"Request failed: {str(e)}")
            span.set_attribute("request_result", "Failed")
            return JSONResponse(status_code=500, content={"detail": "Server Error"})

# Returns or creates database
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY, amount REAL, status TEXT)")
    return conn

# Simulates business transaction.
# Contains configurable fail rate to simulate app failure spikes
@app.get("/process_transaction")
def process_transaction(total: int, fail_rate: float = 0.3):
    with tracer.start_as_current_span("process_transaction") as span:

        logger.info("Starting transaction")
        
        with tracer.start_as_current_span("processing_simultaion") as span1:
            # Simulates processing
            cpu_start = time.time()
            val = 0
            for i in range(500_000):
                val += math.sqrt(i)
        
            total_time = time.time() - cpu_start
            processing_duration.record(total_time)
            logger.info("CPU loop done")
            span1.set_attribute("processing_time", total_time)

        with tracer.start_as_current_span("fraud_check_simulation") as span1:
            # Processing failure simulation
            if random.random() < fail_rate:
                logger.warning("Fraud check failed")
                span1.set_attribute("fraud_check_result", "Failed")
                raise HTTPException(status_code=400, detail="Fraud rejected")
            else:
                span1.set_attribute("fraud_check_result", "Success")

        with tracer.start_as_current_span("database_insert_simulation") as span1:
            # Database record insert
            db_start = time.time()
            try:
                conn = get_db()
                cur = conn.cursor()
                
                table = "fake_db" if random.random() < fail_rate else "transactions"
                cur.execute(f"INSERT INTO {table} (amount, status) VALUES (?, ?)", (total, "SUCCESS"))
                conn.commit()
                conn.close()
                
                transaction_amount_sum.add(total)
                span1.set_attribute("database_insert_result", "Success")
                logger.info("DB insert OK")
                    
            except Exception as e:
                database_error_counter.add(1)
                logger.exception(f"DB error: {str(e)}")
                span1.set_attribute("database_insert_result", "Failed")
                raise HTTPException(status_code=500, detail="DB error")

        span.set_attribute("transacted_amount", total)
        return {"status": "processed"}

# Function to cause CPU high load
@app.get("/maintenance")
def maintenance_task(mult: int = 5):
    with tracer.start_as_current_span("database_insert_simulation") as span:
        logger.info("Starting maintenance")
        start = time.time()
        
        data = [random.random() for _ in range(mult * 100_000)]
        data.sort()
        
        total_time = time.time() - start
        processing_duration.record(total_time)
        logger.info("Maintenance done.")
        span.set_attribute("processing_time", total_time)
        return {"status": "done"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
