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

# CONFIG
request_id_var = ContextVar("request_id", default="system")

class SimpleJsonFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "time": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "req_id": request_id_var.get(),
            "msg": record.getMessage()
        }
        if hasattr(record, "extra_info"):
            log_data.update(record.extra_info)
        return json.dumps(log_data)

DB_PATH = "./demo.db"

LOG_FILE = "/var/log/elk_app.log"

logger = logging.getLogger("elk_app")
logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE)
fh.setFormatter(SimpleJsonFormatter())
logger.addHandler(fh)

logger.propagate = False 

app = FastAPI()


# Logs every request, saving path and execution time
@app.middleware("http")
async def log_requests(request: Request, call_next):
    req_id = str(uuid.uuid4())
    request_id_var.set(req_id)
    
    start = time.time()
    logger.info(f"Request to {request.url.path}")
    
    try:
        response = await call_next(request)
        ms = round((time.time() - start) * 1000, 2)
        logger.info("Request OK", extra={"extra_info": 
                                        {"status": response.status_code, "ms": ms}})
        return response
    except Exception as e:
        ms = round((time.time() - start) * 1000, 2)
        logger.error(f"Request failed: {str(e)}", extra={"extra_info": {"ms": ms}})
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
    logger.info("Starting transaction", extra={"extra_info": {"total": total}})
    
    # Simulates processing
    cpu_start = time.time()
    val = 0
    for i in range(500_000):
        val += math.sqrt(i)
    
    logger.info("CPU loop done", 
                extra=  {"extra_info": 
                            {"cpu_ms": 
                                round((time.time() - cpu_start) * 1000, 2)
                            }
                        }
                )

    # Processing failure simulation
    if random.random() < fail_rate:
        logger.warning("Fraud check failed")
        raise HTTPException(status_code=400, detail="Fraud rejected")

    # Database record insert
    db_start = time.time()
    try:
        conn = get_db()
        cur = conn.cursor()
        
        table = "fake_db" if random.random() < fail_rate else "transactions"
        cur.execute(f"INSERT INTO {table} (amount, status) VALUES (?, ?)", (total, "SUCCESS"))
        conn.commit()
        conn.close()
        
        logger.info("DB insert OK", extra={"extra_info": {"db_ms": round((time.time() - db_start) * 1000, 2), "table": table}})
            
    except Exception as e:
        logger.error(f"DB error: {str(e)}", extra={"extra_info": {"db_ms": round((time.time() - db_start) * 1000, 2)}})
        raise HTTPException(status_code=500, detail="DB error")

    return {"status": "processed", "req_id": request_id_var.get()}

# Function to cause CPU high load
@app.get("/maintenance")
def maintenance_task(mult: int = 5):
    logger.info("Starting maintenance")
    start = time.time()
    
    data = [random.random() for _ in range(mult * 100_000)]
    data.sort()
    
    logger.info("Maintenance done", extra={"extra_info": {"ms": round((time.time() - start) * 1000, 2)}})
    return {"status": "done"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)