"""Main entrypoint for the Kubernetes Benchmark Agent FastAPI application."""

import asyncio
from concurrent import futures
import contextlib
import logging
import os

from api.routes import chromium_density
from api.routes import python_density
from api.routes import python_payload
from api.routes import python_qps
from api.routes import run
import dotenv
import fastapi
import google.cloud.logging as gcl
import uvicorn

dotenv.load_dotenv(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "generated.env")
)

try:
  gcl.Client().setup_logging()
except Exception:  # pylint: disable=broad-exception-caught
  logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def compute_thread_count() -> int:
  """Computes the number of worker threads for the default executor."""
  env_val = os.getenv("FASTAPI_WORKERS")
  if env_val:
    return int(env_val)
  return max(2, min(64, 2 * (os.cpu_count() or 1)))


@contextlib.asynccontextmanager
async def lifespan(unused_app: fastapi.FastAPI):
  """Lifespan context manager for FastAPI application startup and shutdown."""
  workers = compute_thread_count()
  executor = futures.ThreadPoolExecutor(max_workers=workers)
  loop = asyncio.get_running_loop()
  loop.set_default_executor(executor)
  logger.info("Default ThreadPoolExecutor set to %d workers", workers)
  try:
    yield
  finally:
    executor.shutdown(wait=False)


app = fastapi.FastAPI(
    title="Kubernetes Benchmark Agent", version="0.2.0", lifespan=lifespan
)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
  """Health check endpoint."""
  return {"status": "ok"}


app.include_router(python_density.router)
app.include_router(python_payload.router)
app.include_router(python_qps.router)
app.include_router(chromium_density.router)
app.include_router(run.router)

if __name__ == "__main__":
  uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
