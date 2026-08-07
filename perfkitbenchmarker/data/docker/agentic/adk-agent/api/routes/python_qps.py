import asyncio
import logging
import os
import time
import subprocess
from concurrent.futures import ThreadPoolExecutor
from fastapi import APIRouter
from pydantic import BaseModel, Field
from api.utils import benchmark_lock, make_sandbox_client, percentile_stats

logger = logging.getLogger(__name__)
router = APIRouter()

class QpsBenchmarkRequest(BaseModel):
    target_qps: float = Field(default=10.0, ge=0.1, description="Target requests per second")
    duration_s: float = Field(default=60.0, ge=5.0, description="Duration of the QPS burst in seconds")
    sandbox_exec_timeout_s: int = Field(default=30, ge=10, description="Sandbox command execution timeout in seconds")

@router.post("/benchmark/python/qps")
async def benchmark_python_qps(req: QpsBenchmarkRequest):
    basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    qps_script_path = os.path.join(basedir, "sandboxed_apps/python_test_app/benchmark_qps.py")
    try:
        with open(qps_script_path, "r") as f:
            qps_code = f.read()
    except Exception:
        qps_code = "import json; print(json.dumps({'sandbox_status': 'success'}))"

    sandbox_template = os.getenv("SANDBOX_TEMPLATE", "python-sandbox-template")
    sandbox_namespace = os.getenv("AGENTIC_NAMESPACE", "agentic")
    exec_timeout = req.sandbox_exec_timeout_s
    qps_claim_label = {"created-by": "pkb-qps-benchmark"}

    def _run_qps_request(request_id: int) -> dict:
        t_total = time.perf_counter()
        client = make_sandbox_client()
        sandbox = None
        try:
            t0 = time.perf_counter()
            sandbox = client.create_sandbox(template=sandbox_template, namespace=sandbox_namespace, labels=qps_claim_label)
            claim_ms = (time.perf_counter() - t0) * 1000
            t0 = time.perf_counter()
            sandbox.files.write("script.py", qps_code)
            upload_ms = (time.perf_counter() - t0) * 1000
            t0 = time.perf_counter()
            result = sandbox.commands.run("python3 script.py", timeout=exec_timeout)
            exec_ms = (time.perf_counter() - t0) * 1000
            ttfe_ms = (time.perf_counter() - t_total) * 1000
            return {"request_id": request_id, "ttfe_ms": round(ttfe_ms, 3), "claim_ms": round(claim_ms, 3), "upload_ms": round(upload_ms, 3), "exec_ms": round(exec_ms, 3)}
        except Exception as e:
            ttfe_ms = (time.perf_counter() - t_total) * 1000
            return {"request_id": request_id, "ttfe_ms": round(ttfe_ms, 3), "error": f"{type(e).__name__}: {e}"}
        finally:
            if sandbox is not None:
                try:
                    client.delete_sandbox(sandbox.claim_name, namespace=sandbox_namespace)
                except Exception:
                    pass

    async with benchmark_lock:
        logger.info("Starting QPS benchmark: target_qps=%.1f duration_s=%.1f", req.target_qps, req.duration_s)
        interval = 1.0 / req.target_qps
        peak_concurrency = int(req.target_qps * req.duration_s)
        qps_workers = max(16, min(512, peak_concurrency))
        qps_executor = ThreadPoolExecutor(max_workers=qps_workers)
        loop = asyncio.get_running_loop()
        logger.info(
            "QPS executor: %d workers for ~%d expected requests",
            qps_workers, peak_concurrency,
        )
        
        tasks = []
        t_start = time.time()
        next_fire = t_start
        request_id = 0

        while True:
            now = time.time()
            elapsed = now - t_start
            if elapsed >= req.duration_s:
                break
            if now >= next_fire:
                rid = request_id
                request_id += 1
                fut = loop.run_in_executor(qps_executor, _run_qps_request, rid)
                tasks.append(fut)
                next_fire += interval
            else:
                await asyncio.sleep(min(0.001, next_fire - now))

        drain_timeout = max(60.0, req.duration_s)
        done, pending = await asyncio.wait(tasks, timeout=drain_timeout)
        qps_executor.shutdown(wait=False)

        session_results = []
        for t in done:
            try:
                session_results.append(t.result())
            except Exception as exc:
                session_results.append({"request_id": -1, "error": str(exc)})

        for t in pending:
            t.cancel()
        if pending:
            logger.warning("QPS drain timeout: %d/%d requests still pending", len(pending), len(tasks))
            for t in pending:
                session_results.append({"request_id": -1, "error": "drain_timeout"})

        try:
            _claims = subprocess.run(
                ["kubectl", "get", "sandboxclaim", "-n", sandbox_namespace, "-l", "created-by=pkb-qps-benchmark", "-o", "jsonpath={.items[*].metadata.name}"],
                capture_output=True, text=True
            )
            claim_names = _claims.stdout.strip().split()
            if claim_names and claim_names != [""]:
                logger.info("Cleaning up %d lingering pkb-qps claims", len(claim_names))
                subprocess.run(["kubectl", "delete", "sandboxclaim", "-l", "created-by=pkb-qps-benchmark", "-n", sandbox_namespace, "--wait=false"], capture_output=True, text=True)
        except Exception:
            logger.warning("Failed to clean up lingering claims", exc_info=True)

    wall_time = time.time() - t_start
    successful = [r for r in session_results if "error" not in r]
    failed = [r for r in session_results if "error" in r]
    aggregate = {}
    if successful:
        ttfe_values = sorted(r["ttfe_ms"] for r in successful)
        if ttfe_values:
            aggregate.update(percentile_stats(ttfe_values, "ttfe"))
        claim_values = sorted(r["claim_ms"] for r in successful if "claim_ms" in r)
        if claim_values:
            aggregate.update(percentile_stats(claim_values, "claim"))

    return {
        "target_qps": req.target_qps,
        "actual_qps": round(request_id / wall_time, 2) if wall_time > 0 else 0,
        "duration_s": round(wall_time, 2),
        "total_requests": request_id,
        "successful_requests": len(successful),
        "failed_requests": len(failed),
        "aggregate": aggregate,
        "sessions": session_results,
    }
