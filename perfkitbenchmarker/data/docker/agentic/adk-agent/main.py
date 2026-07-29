import os
from dotenv import load_dotenv

# Load env vars BEFORE any imports that depend on them.
# performance_agent/agent.py reads CLUSTER_NAME, GOOGLE_CLOUD_LOCATION,
# AGENTIC_NAMESPACE at import time via os.getenv().
# google.cloud.logging needs GCP project env vars for setup.
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "generated.env"))

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

import google.cloud.logging as gcl
import uvicorn
from fastapi import FastAPI

from api.routes import python_density, python_payload, python_qps, chromium_density, run

try:
    gcl.Client().setup_logging()
except Exception:
    logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def compute_thread_count() -> int:
    env_val = os.getenv("FASTAPI_WORKERS")
    if env_val: return int(env_val)
    return max(2, min(64, 2 * (os.cpu_count() or 1)))

@asynccontextmanager
async def lifespan(app: FastAPI):
    workers = compute_thread_count()
    executor = ThreadPoolExecutor(max_workers=workers)
    loop = asyncio.get_running_loop()
    loop.set_default_executor(executor)
    logger.info("Default ThreadPoolExecutor set to %d workers", workers)
    try:
        yield
    finally:
        executor.shutdown(wait=False)

app = FastAPI(title="Kubernetes Benchmark Agent", version="0.2.0", lifespan=lifespan)

@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}

app.include_router(python_density.router)
app.include_router(python_payload.router)
app.include_router(python_qps.router)
app.include_router(chromium_density.router)
app.include_router(run.router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
