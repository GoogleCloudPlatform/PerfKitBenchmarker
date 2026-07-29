import asyncio
import logging
import os
from fastapi import APIRouter
from pydantic import BaseModel, Field
from api.utils import benchmark_lock, run_single_session, percentile_stats

logger = logging.getLogger(__name__)
router = APIRouter()

class BenchmarkRequest(BaseModel):
    sample_count: int = Field(default=100, ge=1, description="Sample count per sandbox session")
    sample_warmup: int = Field(default=5, ge=0, description="Warmup iterations per sandbox session")
    concurrent_sessions: int = Field(default=1, ge=1, description="Number of parallel sandbox sessions")
    sandbox_exec_timeout_s: int = Field(default=60, ge=10, description="Sandbox command execution timeout in seconds")

@router.post("/benchmark/python/density")
async def benchmark_python_density(req: BenchmarkRequest):
    async with benchmark_lock:
        os.environ["BENCHMARK_MODE"] = "density"
        os.environ["SAMPLE_COUNT"] = str(req.sample_count)
        os.environ["SAMPLE_WARMUP"] = str(req.sample_warmup)
        os.environ["SANDBOX_EXEC_TIMEOUT_S"] = str(req.sandbox_exec_timeout_s)

        logger.info("Starting Python benchmark: sample_count=%d sample_warmup=%d concurrent_sessions=%d", req.sample_count, req.sample_warmup, req.concurrent_sessions)
        prompt = "start"
        
        thread_tasks = [
            asyncio.create_task(asyncio.to_thread(lambda sid=i: asyncio.run(run_single_session(sid, prompt))))
            for i in range(req.concurrent_sessions)
        ]
        session_results = await asyncio.gather(*thread_tasks)

    successful = [r for r in session_results if "error" not in r]
    failed = [r for r in session_results if "error" in r]
    aggregate = {}
    if successful:
        orch_times = sorted(r["orchestrator_total_ms"] for r in successful)
        aggregate.update(percentile_stats(orch_times, "orchestrator_cel"))
        sandbox_keys = [k for k in successful[0] if k.startswith("sandbox_")]
        for key in sandbox_keys:
            sample_val = successful[0].get(key)
            if isinstance(sample_val, list):
                pooled = sorted(v for r in successful for v in (r.get(key) or []) if isinstance(r.get(key), list))
                if pooled:
                    base = key[:-3] if key.endswith("_ms") else key
                    aggregate.update(percentile_stats(pooled, base))
            elif isinstance(sample_val, (int, float)):
                vals = [r[key] for r in successful if key in r and isinstance(r[key], (int, float))]
                if vals:
                    if key.endswith("_cel_ms"):
                        base = key[:-3]
                        aggregate.update(percentile_stats(sorted(vals), base))
                    else:
                        aggregate[key] = round(sum(vals) / len(vals), 6)

    return {
        "concurrent_sessions": req.concurrent_sessions,
        "successful_sessions": len(successful),
        "failed_sessions": len(failed),
        "aggregate": aggregate,
        "sessions": session_results,
    }
