"""Python payload benchmark routes."""

import asyncio
import logging
import os
from typing import Any

from api import utils
import fastapi
import pydantic

logger = logging.getLogger(__name__)
router = fastapi.APIRouter()


class PayloadBenchmarkRequest(pydantic.BaseModel):
  """Request parameters for Python payload benchmark."""

  payload_size_mb: float = pydantic.Field(
      default=1, gt=0, description="Payload size in MB"
  )
  payload_iterations: int = pydantic.Field(
      default=20, ge=1, description="Number of transfer iterations"
  )
  concurrent_sessions: int = pydantic.Field(
      default=1, ge=1, description="Number of parallel sandbox sessions"
  )
  sandbox_exec_timeout_s: int = pydantic.Field(
      default=60,
      ge=10,
      description="Sandbox command execution timeout in seconds",
  )


@router.post("/benchmark/python/payload")
async def benchmark_python_payload(
    req: PayloadBenchmarkRequest,
) -> dict[str, Any]:
  """Runs the Python payload benchmark."""
  async with utils.benchmark_lock:
    os.environ["BENCHMARK_MODE"] = "payload"
    os.environ["PAYLOAD_SIZE_MB"] = str(req.payload_size_mb)
    os.environ["PAYLOAD_ITERATIONS"] = str(req.payload_iterations)
    os.environ["SANDBOX_EXEC_TIMEOUT_S"] = str(req.sandbox_exec_timeout_s)

    logger.info(
        "Starting Payload benchmark: payload_size_mb=%s iterations=%d"
        " concurrent_sessions=%d",
        req.payload_size_mb,
        req.payload_iterations,
        req.concurrent_sessions,
    )
    prompt = (
        "(Unused non-empty prompt just to satisfy ADK Runner.run_async"
        " requirements)"
    )

    thread_tasks = [
        asyncio.create_task(
            asyncio.to_thread(
                lambda sid=i: asyncio.run(utils.run_single_session(sid, prompt))
            )
        )
        for i in range(req.concurrent_sessions)
    ]
    session_results = await asyncio.gather(*thread_tasks)

  successful = [r for r in session_results if "error" not in r]
  failed = [r for r in session_results if "error" in r]
  aggregate = {}
  if successful:
    orch_times = sorted(r["orchestrator_total_ms"] for r in successful)
    aggregate.update(
        utils.percentile_stats(orch_times, "orchestrator_transfer")
    )
    sandbox_keys = [k for k in successful[0] if k.startswith("sandbox_")]
    for key in sandbox_keys:
      vals = [
          r[key]
          for r in successful
          if key in r and isinstance(r[key], (int, float))
      ]
      if vals:
        aggregate[key] = round(sum(vals) / len(vals), 6)

  return {
      "payload_size_mb": req.payload_size_mb,
      "payload_iterations": req.payload_iterations,
      "concurrent_sessions": req.concurrent_sessions,
      "successful_sessions": len(successful),
      "failed_sessions": len(failed),
      "aggregate": aggregate,
      "sessions": session_results,
  }
