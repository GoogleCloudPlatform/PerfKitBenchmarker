"""FastAPI service fronting the GKE Performance Agent.

Exposes REST endpoints that PKB calls to trigger benchmarks.  The agent
runs *inside* the GKE cluster so it can reach the Sandbox Controller and
create gVisor sandboxes natively.

Endpoints:
  GET  /healthz                      → liveness probe
  POST /benchmark/python/density     → run the Python density benchmark
  POST /benchmark/python/payload     → run the payload transfer benchmark
  POST /benchmark/python/qps         → run the QPS saturation benchmark
  POST /benchmark/chromium/density   → run the Chromium density benchmark
  POST /run                          → raw ADK agent interaction

POST /benchmark/python/density — Request:
  {
    "sample_count":            int — iterations per sandbox session (default: 100)
    "sample_warmup":           int — warmup iterations excluded from stats (default: 5)
    "concurrent_sessions":     int — parallel sandbox sessions (default: 1)
    "sandbox_exec_timeout_s":  int — sandbox command execution timeout in seconds (default: 60)
  }

POST /benchmark/python/density — Response:
  {
    "concurrent_sessions":  int — requested session count
    "successful_sessions":  int — sessions completed without error
    "failed_sessions":      int — sessions that returned an error
    "aggregate": {
      --- Orchestrator-side (timed in _run_single_session, stats in benchmark_density) ---
      "orchestrator_cel_mean_ms":  mean round-trip across sessions
      "orchestrator_cel_p50_ms":   P50 round-trip
      "orchestrator_cel_p99_ms":   P99 round-trip
      "orchestrator_cel_min_ms":   min round-trip
      "orchestrator_cel_max_ms":   max round-trip

      --- Sandbox-side overall (from benchmark_density.py, mean across sessions) ---
      "sandbox_ttfe_ms":               Time To First Execution
      "sandbox_total_cel_mean_ms":     mean total CEL per iteration (sum of all task types)
      "sandbox_total_cel_p50_ms":      P50 total CEL per iteration
      "sandbox_total_cel_p99_ms":      P99 total CEL per iteration
      "sandbox_total_cel_min_ms":      min total CEL per iteration
      "sandbox_total_cel_max_ms":      max total CEL per iteration

      --- Sandbox RSS (from benchmark_density.py, mean across sessions) ---
      "sandbox_rss_start_mb":      RSS at benchmark start
      "sandbox_rss_end_mb":        RSS at benchmark end
      "sandbox_rss_growth_mb":     RSS growth during benchmark

      --- Per-type CEL breakdown (from benchmark_density.py, mean across sessions) ---
      "sandbox_compute_cel_{mean,p50,p99,min,max}_ms":  CPU-bound (math.factorial)
      "sandbox_syscall_cel_{mean,p50,p99,min,max}_ms":  gVisor Sentry (os.stat/listdir)
      "sandbox_import_cel_{mean,p50,p99,min,max}_ms":   Gofer FS I/O (importlib)
    }
    "sessions": [             per-session detail array
      {
        "session_id":           int — zero-based session index
        "orchestrator_total_ms": float — full round-trip for this session
        "raw_output":           str — raw code execution stdout
        "sandbox_ttfe_ms":      float — TTFE for this session
        "sandbox_total_cel_mean_ms":  float — total CEL mean for this session
        ...                     all other sandbox_* metrics for this session
      }
    ]
  }

Data Flow:
  benchmark_density.py (inside gVisor)  → all sandbox_* metrics per session
  main.py (this file)                  → orchestrator_* timing + cross-session aggregation
"""

from __future__ import annotations


import json
import logging
import os
import re
import time
import asyncio
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
import subprocess

import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from google.genai import types
from google.adk.sessions import InMemorySessionService
from google.adk.artifacts import InMemoryArtifactService
from google.adk.runners import Runner

from dotenv import load_dotenv
from k8s_agent_sandbox.sandbox_client import SandboxClient
from k8s_agent_sandbox.models import SandboxDirectConnectionConfig
from playwright.async_api import async_playwright
from kubernetes import client as k8s_client, config as k8s_config
import google.cloud.logging as gcl

basedir = os.path.abspath(os.path.dirname(__file__))

# Load generated.env (rendered by gke_image_build_utils._GenerateEnvFile from PKB flags).
# In GKE, K8s manifest env vars take precedence.
load_dotenv(os.path.join(basedir, "generated.env"))

from gke_performance_agent import agent


# ── SandboxClient factory (DirectConnection vs Dev-mode tunnel) ──────────
def _make_sandbox_client() -> SandboxClient:
    """Create a SandboxClient with the optimal connection strategy.

    When SANDBOX_ROUTER_URL is set (in-cluster), uses DirectConnectionConfig
    to bypass kubectl port-forward SPDY tunnels — enabling true N-way
    parallelism.  Without it, falls back to LocalTunnelConnectionConfig
    (dev mode, serialized through a single SPDY stream).
    """
    router_url = os.getenv("SANDBOX_ROUTER_URL")
    if router_url:

        return SandboxClient(
            connection_config=SandboxDirectConnectionConfig(api_url=router_url)
        )
    return SandboxClient()


# --- Constants ---
APP_NAME = "gke_performance_agent_app"
USER_ID = "benchmark_user"

# --- Configure Logging ---
try:
    gcl.Client().setup_logging()
except Exception:
    logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =========================================================================
# FastAPI Application
# =========================================================================
# --- Adaptive ThreadPool based on Agent CPU ---
def _compute_thread_count() -> int:
    """Compute a recommended max worker count for ThreadPoolExecutor.

    Heuristic: use ~2x the detected CPU count to provide overlap for blocking
    I/O (port-forward, file upload) while avoiding CPU oversubscription.
    Cap between 2 and 64 workers.
    """
    cpu = os.cpu_count() or 1
    return max(2, min(64, cpu * 2))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan handler: configure a tuned ThreadPoolExecutor for asyncio.

    Sets the default executor so `asyncio.to_thread` uses our tuned pool,
    and shuts it down on application exit.
    """
    workers = _compute_thread_count()
    executor = ThreadPoolExecutor(max_workers=workers)
    loop = asyncio.get_running_loop()
    loop.set_default_executor(executor)
    logging.info(
        "Default ThreadPoolExecutor set to %d workers (cpu=%s)", workers, os.cpu_count()
    )
    try:
        yield
    finally:
        try:
            executor.shutdown(wait=False)
            logging.info("ThreadPoolExecutor shut down")
        except Exception:
            logging.exception("Error shutting down ThreadPoolExecutor")


app = FastAPI(title="GKE Benchmark Agent", version="0.2.0", lifespan=lifespan)

# Serialise benchmark requests so concurrent POSTs cannot clobber the
# shared env vars (BENCHMARK_MODE, SAMPLE_COUNT, …) that agent.py reads.
_benchmark_lock = asyncio.Lock()


def _percentile_stats(sorted_values: list, prefix: str) -> dict:
    """Compute mean/p50/p95/p99/min/max from a pre-sorted list of numbers."""
    n = len(sorted_values)
    if n == 0:
        return {}
    return {
        f"{prefix}_mean_ms": round(sum(sorted_values) / n, 6),
        f"{prefix}_p50_ms": round(sorted_values[n // 2], 6),
        f"{prefix}_p95_ms": round(sorted_values[min(int(n * 0.95), n - 1)], 6),
        f"{prefix}_p99_ms": round(sorted_values[min(int(n * 0.99), n - 1)], 6),
        f"{prefix}_min_ms": round(sorted_values[0], 6),
        f"{prefix}_max_ms": round(sorted_values[-1], 6),
    }


# --- Request / Response Models ---
class BenchmarkRequest(BaseModel):
    sample_count: int = Field(
        default=100, ge=1, description="Sample count per sandbox session"
    )
    sample_warmup: int = Field(
        default=5, ge=0, description="Warmup iterations per sandbox session"
    )
    concurrent_sessions: int = Field(
        default=1, ge=1, description="Number of parallel sandbox sessions"
    )
    sandbox_exec_timeout_s: int = Field(
        default=60, ge=10, description="Sandbox command execution timeout in seconds"
    )


class RunRequest(BaseModel):
    prompt: str = "Please start the GKE performance benchmark workflow."


class PayloadBenchmarkRequest(BaseModel):
    payload_size_mb: float = Field(default=1, gt=0, description="Payload size in MB")
    payload_iterations: int = Field(
        default=20, ge=1, description="Number of transfer iterations"
    )
    concurrent_sessions: int = Field(
        default=1, ge=1, description="Number of parallel sandbox sessions"
    )
    sandbox_exec_timeout_s: int = Field(
        default=60, ge=10, description="Sandbox command execution timeout in seconds"
    )


class QpsBenchmarkRequest(BaseModel):
    target_qps: float = Field(
        default=10.0, ge=0.1, description="Target requests per second"
    )
    duration_s: float = Field(
        default=60.0, ge=5.0, description="Duration of the QPS burst in seconds"
    )
    sandbox_exec_timeout_s: int = Field(
        default=30, ge=10, description="Sandbox command execution timeout in seconds"
    )


class ChromiumBenchmarkRequest(BaseModel):
    task_count: int = Field(
        default=10, ge=1, description="Iterations per Chromium session"
    )
    warmup_tasks: int = Field(
        default=2, ge=0, description="Warmup iterations excluded from stats"
    )
    concurrent_sessions: int = Field(
        default=1, ge=1, description="Number of parallel Chromium sessions"
    )
    sandbox_exec_timeout_s: int = Field(
        default=120, ge=10, description="Sandbox command execution timeout in seconds"
    )


# --- JSON extraction helper ---
_JSON_RE = re.compile(r"\{[^{}]*\}", re.DOTALL)


def _parse_sandbox_json(raw_output: str) -> Optional[dict]:
    """Extract the sandbox JSON summary from code execution output.

    The sandbox script prints a JSON blob to stdout among other log lines.
    We find the last valid JSON object that contains sandbox_ keys.
    """
    matches = _JSON_RE.findall(raw_output)
    for candidate in reversed(matches):
        try:
            obj = json.loads(candidate)
            if any(k.startswith("sandbox_") for k in obj):
                return obj
        except json.JSONDecodeError:
            continue
    return None


# --- Agent helper ---
async def _run_agent(prompt: str) -> str:
    """Create a fresh session, run the agent, return the final text output."""
    session_service = InMemorySessionService()
    artifact_service = InMemoryArtifactService()
    session = await session_service.create_session(
        app_name=APP_NAME,
        user_id=USER_ID,
        state={},
    )

    runner = Runner(
        agent=agent.root_agent,
        app_name=APP_NAME,
        session_service=session_service,
        artifact_service=artifact_service,
    )

    content = types.Content(
        role="user",
        parts=[types.Part(text=prompt)],
    )

    final_response = ""
    code_execution_output = ""
    async with runner:
        async for event in runner.run_async(
            user_id=USER_ID,
            session_id=session.id,
            new_message=content,
        ):
            if event.content and event.content.parts:
                for part in event.content.parts:
                    cer = getattr(part, "code_execution_result", None) or getattr(
                        part, "codeExecutionResult", None
                    )
                    if cer:
                        code_execution_output = getattr(cer, "output", "") or ""
            if event.is_final_response() and event.content and event.content.parts:
                final_response = event.content.parts[0].text

    await session_service.delete_session(
        app_name=APP_NAME,
        user_id=USER_ID,
        session_id=session.id,
    )
    return code_execution_output if code_execution_output else final_response


async def _run_single_session(session_id: int, prompt: str) -> dict:
    """Run one agent session and return orchestrator + sandbox metrics."""
    orchestrator_start = time.perf_counter()
    logging.info("SESSION_START: session_id=%d start_ts=%.3f", session_id, time.time())

    try:
        raw_output = await _run_agent(prompt)
    except Exception as e:
        return {
            "session_id": session_id,
            "error": str(e),
        }

    orchestrator_elapsed_ms = round(
        (time.perf_counter() - orchestrator_start) * 1000, 6
    )
    logging.info(
        "SESSION_END: session_id=%d elapsed_ms=%.3f",
        session_id,
        orchestrator_elapsed_ms,
    )

    # Parse sandbox-side metrics from the code execution output
    sandbox_metrics = _parse_sandbox_json(raw_output) or {}

    return {
        "session_id": session_id,
        "orchestrator_total_ms": orchestrator_elapsed_ms,
        "raw_output": raw_output,
        **sandbox_metrics,
    }


# --- Endpoints ---
@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@app.post("/benchmark/python/density")
async def benchmark_python_density(req: BenchmarkRequest):
    """Trigger the Python density benchmark (Use Case B).

    Fires `concurrent_sessions` parallel agent sessions.  Each session
    claims its own sandbox, runs the benchmark script with the given
    iteration/warmup counts, and returns both orchestrator-side and
    sandbox-side metrics.
    """
    async with _benchmark_lock:
        os.environ["BENCHMARK_MODE"] = "density"
        os.environ["SAMPLE_COUNT"] = str(req.sample_count)
        os.environ["SAMPLE_WARMUP"] = str(req.sample_warmup)
        os.environ["SANDBOX_EXEC_TIMEOUT_S"] = str(req.sandbox_exec_timeout_s)

        logger.info(
            "Starting Python benchmark: sample_count=%d sample_warmup=%d concurrent_sessions=%d",
            req.sample_count,
            req.sample_warmup,
            req.concurrent_sessions,
        )

        prompt = "Please start the GKE performance benchmark workflow."

        # Fire concurrent sessions.
        # DESIGN NOTE: Each session runs in its own thread via asyncio.to_thread()
        # with a nested asyncio.run() to create a per-thread event loop. This is
        # intentional -- the ADK Runner performs blocking I/O (sandbox lifecycle
        # via kubectl/HTTP) that would starve a shared event loop and serialize
        # session starts. The per-thread event loop overhead (~0.1ms) is negligible
        # compared to sandbox round-trip times (~200ms+).
        thread_tasks = [
            asyncio.create_task(
                asyncio.to_thread(
                    lambda sid=i: asyncio.run(_run_single_session(sid, prompt))
                )
            )
            for i in range(req.concurrent_sessions)
        ]
        session_results = await asyncio.gather(*thread_tasks)

    # Separate successful vs failed sessions
    successful = [r for r in session_results if "error" not in r]
    failed = [r for r in session_results if "error" in r]

    # Aggregate orchestrator-side metrics across all successful sessions
    aggregate = {}
    if successful:
        orch_times = sorted(r["orchestrator_total_ms"] for r in successful)
        aggregate.update(_percentile_stats(orch_times, "orchestrator_cel"))

        # Aggregate sandbox-side metrics across sessions
        sandbox_keys = [k for k in successful[0] if k.startswith("sandbox_")]
        for key in sandbox_keys:
            sample_val = successful[0].get(key)
            if isinstance(sample_val, list):
                # Pool raw latency arrays across sandboxes → true cross-sandbox stats
                pooled = sorted(
                    v
                    for r in successful
                    for v in (r.get(key) or [])
                    if isinstance(r.get(key), list)
                )
                if pooled:
                    base = key[:-3] if key.endswith("_ms") else key
                    aggregate.update(_percentile_stats(pooled, base))
            elif isinstance(sample_val, (int, float)):
                vals = [
                    r[key]
                    for r in successful
                    if key in r and isinstance(r[key], (int, float))
                ]
                if vals:
                    if key.endswith("_cel_ms"):
                        # Latency scalars (e.g. import_cel_ms): compute
                        # cross-sandbox percentile stats, like array metrics.
                        base = key[:-3]
                        aggregate.update(_percentile_stats(sorted(vals), base))
                    else:
                        # Non-latency scalars (e.g. rss_mb, ttfe_ms): average
                        aggregate[key] = round(sum(vals) / len(vals), 6)

    return {
        "concurrent_sessions": req.concurrent_sessions,
        "successful_sessions": len(successful),
        "failed_sessions": len(failed),
        "aggregate": aggregate,
        "sessions": session_results,
    }


@app.post("/benchmark/python/payload")
async def benchmark_python_payload(req: PayloadBenchmarkRequest):
    """Trigger the payload transfer benchmark (Use Case D).

    Measures the cost of returning large observation payloads from a
    gVisor sandbox back to the orchestrator.  Each session generates a
    payload of `payload_size_mb` MB, encodes it (base64), writes it
    through the gVisor Gofer path, and reports latency breakdowns.
    """
    async with _benchmark_lock:
        os.environ["BENCHMARK_MODE"] = "payload"
        os.environ["PAYLOAD_SIZE_MB"] = str(req.payload_size_mb)
        os.environ["PAYLOAD_ITERATIONS"] = str(req.payload_iterations)
        os.environ["SANDBOX_EXEC_TIMEOUT_S"] = str(req.sandbox_exec_timeout_s)

        logger.info(
            "Starting Payload benchmark: payload_size_mb=%s iterations=%d concurrent_sessions=%d",
            req.payload_size_mb,
            req.payload_iterations,
            req.concurrent_sessions,
        )

        prompt = "Please start the GKE performance benchmark workflow."

        # Fire concurrent sessions.
        # DESIGN NOTE: Each session runs in its own thread via asyncio.to_thread()
        # with a nested asyncio.run() to create a per-thread event loop. This is
        # intentional -- the ADK Runner performs blocking I/O (sandbox lifecycle
        # via kubectl/HTTP) that would starve a shared event loop and serialize
        # session starts. The per-thread event loop overhead (~0.1ms) is negligible
        # compared to sandbox round-trip times (~200ms+).
        thread_tasks = [
            asyncio.create_task(
                asyncio.to_thread(
                    lambda sid=i: asyncio.run(_run_single_session(sid, prompt))
                )
            )
            for i in range(req.concurrent_sessions)
        ]
        session_results = await asyncio.gather(*thread_tasks)

    # Separate successful vs failed sessions
    successful = [r for r in session_results if "error" not in r]
    failed = [r for r in session_results if "error" in r]

    # Aggregate orchestrator-side metrics across all successful sessions
    aggregate = {}
    if successful:
        orch_times = sorted(r["orchestrator_total_ms"] for r in successful)
        aggregate.update(_percentile_stats(orch_times, "orchestrator_transfer"))

        # Aggregate sandbox-side metrics (mean across sessions, numeric only)
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


@app.post("/benchmark/python/qps")
async def benchmark_python_qps(req: QpsBenchmarkRequest):
    """Trigger the QPS saturation benchmark (Use Case F).

    Fires sandbox claim requests at a controlled rate (target_qps) for
    duration_s seconds.  Each request claims a sandbox from the warm pool,
    runs a trivial script, and releases it.  Returns per-request TTFE
    (claim + upload + execute + delete) and aggregate latency stats.

    Uses a lightweight path that calls SandboxClient directly — bypasses
    the full ADK Runner/MockLLM pipeline to avoid per-request overhead
    and accurately measure sandbox lifecycle latency at high QPS.

    When the warm pool drains faster than it refills, TTFE spikes from
    ~200ms to seconds — identifying the QPS saturation point.
    """

    # Load the QPS script once
    qps_script_path = os.path.join(
        basedir, "sandboxed_apps/python_test_app/benchmark_qps.py"
    )
    try:
        with open(qps_script_path, "r") as f:
            qps_code = f.read()
    except Exception:
        qps_code = "import json; print(json.dumps({'sandbox_status': 'ok'}))"

    sandbox_template = os.getenv("SANDBOX_TEMPLATE", "python-sandbox-template")
    sandbox_namespace = os.getenv("AGENTIC_NAMESPACE", "agentic")
    exec_timeout = req.sandbox_exec_timeout_s
    qps_claim_label = {"created-by": "pkb-qps-benchmark"}

    def _run_qps_request(request_id: int) -> dict:
        """Lightweight sandbox claim→execute→release cycle."""
        t_total = time.perf_counter()
        client = _make_sandbox_client()
        sandbox = None
        try:
            # Claim
            t0 = time.perf_counter()
            sandbox = client.create_sandbox(
                template=sandbox_template,
                namespace=sandbox_namespace,
                labels=qps_claim_label,
            )
            claim_ms = (time.perf_counter() - t0) * 1000

            # Upload
            t0 = time.perf_counter()
            sandbox.files.write("script.py", qps_code)
            upload_ms = (time.perf_counter() - t0) * 1000

            # Execute
            t0 = time.perf_counter()
            result = sandbox.commands.run("python3 script.py", timeout=exec_timeout)
            exec_ms = (time.perf_counter() - t0) * 1000

            ttfe_ms = (time.perf_counter() - t_total) * 1000

            return {
                "request_id": request_id,
                "ttfe_ms": round(ttfe_ms, 3),
                "claim_ms": round(claim_ms, 3),
                "upload_ms": round(upload_ms, 3),
                "exec_ms": round(exec_ms, 3),
            }
        except Exception as e:
            ttfe_ms = (time.perf_counter() - t_total) * 1000
            return {
                "request_id": request_id,
                "ttfe_ms": round(ttfe_ms, 3),
                "error": f"{type(e).__name__}: {e}",
            }
        finally:
            if sandbox is not None:
                try:
                    client.delete_sandbox(
                        sandbox.claim_name, namespace=sandbox_namespace
                    )
                except Exception:
                    pass

    async with _benchmark_lock:
        logger.info(
            "Starting QPS benchmark: target_qps=%.1f duration_s=%.1f",
            req.target_qps,
            req.duration_s,
        )

        interval = 1.0 / req.target_qps

        # Use a scoped executor sized to the expected concurrency.
        # Each sandbox request takes ~0.5-5s depending on environment
        # (in-cluster vs port-forward).  We need enough workers so the
        # thread pool itself is never the bottleneck — only real sandbox
        # contention should limit throughput.
        peak_concurrency = int(req.target_qps * req.duration_s)
        qps_workers = max(16, min(512, peak_concurrency))
        qps_executor = ThreadPoolExecutor(max_workers=qps_workers)
        loop = asyncio.get_running_loop()
        logger.info(
            "QPS executor: %d workers for ~%d expected requests",
            qps_workers,
            peak_concurrency,
        )

        # Schedule requests at the target QPS rate
        tasks: list[asyncio.Task] = []
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

        # Wait for in-flight requests with a drain timeout.
        drain_timeout = max(60.0, req.duration_s)
        done, pending = await asyncio.wait(tasks, timeout=drain_timeout)

        # Clean up the scoped executor
        qps_executor.shutdown(wait=False)

        # Collect completed results (guard against individual task exceptions)
        session_results = []
        for t in done:
            try:
                session_results.append(t.result())
            except Exception as exc:
                session_results.append(
                    {
                        "request_id": -1,
                        "error": str(exc),
                    }
                )

        # Cancel tasks still queued/running and mark as timed out
        for t in pending:
            t.cancel()
        if pending:
            logger.warning(
                "QPS drain timeout: %d/%d requests still pending after %.0fs",
                len(pending),
                len(tasks),
                drain_timeout,
            )
            for t in pending:
                session_results.append(
                    {
                        "request_id": -1,
                        "error": "drain_timeout",
                    }
                )

        # Bulk-delete SandboxClaims left by cancelled tasks.
        # Only targets claims labelled created-by=pkb-qps-benchmark so
        # we never touch claims created by other workloads.
        try:
            _claims = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "sandboxclaim",
                    "-n",
                    sandbox_namespace,
                    "-l",
                    "created-by=pkb-qps-benchmark",
                    "-o",
                    "jsonpath={.items[*].metadata.name}",
                ],
                capture_output=True,
                text=True,
            )
            claim_names = _claims.stdout.strip().split()
            if claim_names and claim_names != [""]:
                logger.info("Cleaning up %d lingering pkb-qps claims", len(claim_names))
                subprocess.run(
                    [
                        "kubectl",
                        "delete",
                        "sandboxclaim",
                        "-l",
                        "created-by=pkb-qps-benchmark",
                        "-n",
                        sandbox_namespace,
                        "--wait=false",
                    ],
                    capture_output=True,
                    text=True,
                )
        except Exception:
            logger.warning("Failed to clean up lingering claims", exc_info=True)

    wall_time = time.time() - t_start

    # Separate successful vs failed
    successful = [r for r in session_results if "error" not in r]
    failed = [r for r in session_results if "error" in r]

    # Compute TTFE stats
    aggregate = {}
    if successful:
        ttfe_values = sorted(r["ttfe_ms"] for r in successful)
        if ttfe_values:
            aggregate.update(_percentile_stats(ttfe_values, "ttfe"))

        # Also compute claim latency stats (the warm-pool-sensitive metric)
        claim_values = sorted(r["claim_ms"] for r in successful if "claim_ms" in r)
        if claim_values:
            aggregate.update(_percentile_stats(claim_values, "claim"))

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


@app.post("/benchmark/chromium/density")
async def benchmark_chromium_density(req: ChromiumBenchmarkRequest):
    """Trigger the Chromium density benchmark (Use Case C).

    Fires `concurrent_sessions` parallel Chromium sandbox sessions.  Each
    session claims its own sandbox from the chromium warm pool, connects to
    the sandbox's Chrome instance via CDP (Chrome DevTools Protocol), and
    drives the benchmark from the orchestrator using Playwright.

    Architecture:
      - Sandbox: runs headless Chromium (upstream chrome-sandbox image) with
        --remote-debugging-port=9222 --remote-debugging-address=0.0.0.0
      - Orchestrator: connects Playwright via connect_over_cdp() to the
        sandbox pod IP:9222 and drives navigate/click/evaluate/screenshot.
      - This isolates pure Chrome-under-gVisor overhead without Node.js or
        a runtime server in the sandbox.
    """
    from playwright.async_api import async_playwright
    from kubernetes import client as k8s_client, config as k8s_config

    async with _benchmark_lock:

        sandbox_namespace = os.getenv("AGENTIC_NAMESPACE", "agentic")
        sandbox_template = "chromium-sandbox-template"

        logger.info(
            "Starting Chromium density benchmark (CDP): concurrent_sessions=%d "
            "task_count=%d warmup_tasks=%d",
            req.concurrent_sessions,
            req.task_count,
            req.warmup_tasks,
        )

        # Initialize K8s client for pod IP lookup
        try:
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            k8s_config.load_kube_config()
        core_v1 = k8s_client.CoreV1Api()

        # Inline HTML test page (data: URL avoids network dependencies)
        test_page = """data:text/html,
<!DOCTYPE html>
<html>
<head><title>PKB Chromium Benchmark</title></head>
<body>
  <h1 id="heading">Hello Sandbox</h1>
  <input id="search" type="text" placeholder="Search..." />
  <button id="btn">Click Me</button>
  <div id="output"></div>
  <script>
    document.getElementById('btn').addEventListener('click', () => {
      document.getElementById('output').textContent = 'clicked';
    });
  </script>
</body>
</html>"""

        # Limit concurrent K8s Metrics API calls to avoid overwhelming metrics-server
        _metrics_semaphore = asyncio.Semaphore(5)

        async def _run_chromium_session_cdp(session_id: int) -> dict:
            """Run one Chromium benchmark session via CDP."""
            sb_client = _make_sandbox_client()
            sandbox = None
            t_start = time.time()
            claim_ms = 0.0
            cold_start_ms = 0.0
            try:
                # 1. Claim sandbox from warm pool
                t0 = time.time()
                sandbox = sb_client.create_sandbox(
                    template=sandbox_template,
                    namespace=sandbox_namespace,
                )
                claim_ms = (time.time() - t0) * 1000.0

                # 2. Resolve pod IP
                pod_name = sandbox.get_pod_name()
                pod = core_v1.read_namespaced_pod(pod_name, sandbox_namespace)
                pod_ip = pod.status.pod_ip
                if not pod_ip:
                    raise RuntimeError(f"Pod {pod_name} has no IP assigned")

                cdp_url = f"http://{pod_ip}:9223"

                # 3. Connect Playwright via CDP
                async with async_playwright() as pw:
                    # Wait for Chrome to be ready (retry connection)
                    browser = None
                    for attempt in range(20):
                        try:
                            browser = await pw.chromium.connect_over_cdp(cdp_url)
                            break
                        except Exception:
                            if attempt >= 19:
                                raise
                            await asyncio.sleep(0.5)

                    # Cold start = claim + CDP connect (time until browser ready)
                    cold_start_ms = (time.time() - t_start) * 1000.0

                    context = await browser.new_context()
                    page = await context.new_page()

                    # Navigate once before measurement loop
                    await page.goto(test_page, wait_until="domcontentloaded")

                    # Latency arrays (filled during measured runs only)
                    navigate_ms = []
                    screenshot_ms = []
                    evaluate_ms = []
                    click_ms = []
                    fill_ms = []
                    interaction_ms = []

                    total_runs = req.warmup_tasks + req.task_count
                    for run_idx in range(total_runs):
                        measuring = run_idx >= req.warmup_tasks

                        # 1. Navigate (reload page)
                        t0 = time.time()
                        await page.goto(test_page, wait_until="domcontentloaded")
                        elapsed = (time.time() - t0) * 1000.0
                        if measuring:
                            navigate_ms.append(elapsed)
                            interaction_ms.append(elapsed)

                        # 2. DOM evaluate — read heading text
                        t0 = time.time()
                        await page.evaluate(
                            "() => document.getElementById('heading').textContent"
                        )
                        elapsed = (time.time() - t0) * 1000.0
                        if measuring:
                            evaluate_ms.append(elapsed)
                            interaction_ms.append(elapsed)

                        # 3. Fill input
                        t0 = time.time()
                        await page.fill("#search", f"query-{run_idx}")
                        elapsed = (time.time() - t0) * 1000.0
                        if measuring:
                            fill_ms.append(elapsed)
                            interaction_ms.append(elapsed)

                        # 4. Click button
                        t0 = time.time()
                        await page.click("#btn")
                        elapsed = (time.time() - t0) * 1000.0
                        if measuring:
                            click_ms.append(elapsed)
                            interaction_ms.append(elapsed)

                        # 5. Verify click effect (DOM mutation)
                        t0 = time.time()
                        await page.evaluate(
                            "() => document.getElementById('output').textContent"
                        )
                        elapsed = (time.time() - t0) * 1000.0
                        if measuring:
                            evaluate_ms.append(elapsed)
                            interaction_ms.append(elapsed)

                        # 6. Screenshot
                        t0 = time.time()
                        await page.screenshot()
                        elapsed = (time.time() - t0) * 1000.0
                        if measuring:
                            screenshot_ms.append(elapsed)
                            interaction_ms.append(elapsed)

                    # Read pod memory usage from K8s Metrics API
                    # Retry: metrics-server scrapes every 15-60s; short-lived
                    # sessions may finish before the first scrape (404).
                    rss_mb = None
                    for _metrics_attempt in range(4):
                        try:
                            async with _metrics_semaphore:
                                custom_api = k8s_client.CustomObjectsApi()
                                pod_metrics = await asyncio.to_thread(
                                    custom_api.get_namespaced_custom_object,
                                    group="metrics.k8s.io",
                                    version="v1beta1",
                                    namespace=sandbox_namespace,
                                    plural="pods",
                                    name=pod_name,
                                )
                            for c in pod_metrics.get("containers", []):
                                usage = c.get("usage", {}).get("memory", "")
                                if usage.endswith("Ki"):
                                    rss_mb = round(int(usage[:-2]) / 1024, 1)
                                elif usage.endswith("Mi"):
                                    rss_mb = round(float(usage[:-2]), 1)
                                elif usage.endswith("Gi"):
                                    rss_mb = round(float(usage[:-2]) * 1024, 1)
                                break
                            break  # success
                        except k8s_client.exceptions.ApiException as e:
                            if e.status == 404 and _metrics_attempt < 3:
                                logger.info(
                                    "Metrics not yet available for %s "
                                    "(attempt %d/4, retrying in 5s)",
                                    pod_name,
                                    _metrics_attempt + 1,
                                )
                                await asyncio.sleep(5)
                                continue
                            logger.warning(
                                "Failed to read pod metrics for %s",
                                pod_name,
                                exc_info=True,
                            )
                            break
                        except Exception:
                            logger.warning(
                                "Failed to read pod metrics for %s",
                                pod_name,
                                exc_info=True,
                            )
                            break

                    await browser.close()

                total_ms = (time.time() - t_start) * 1000.0

                # Compute stats helper
                def _compute_stats(arr):
                    if not arr:
                        return None
                    s = sorted(arr)
                    n = len(s)
                    return {
                        "mean_ms": round(sum(s) / n, 3),
                        "p50_ms": round(s[min(int(n * 0.50), n - 1)], 3),
                        "p95_ms": round(s[min(int(n * 0.95), n - 1)], 3),
                        "p99_ms": round(s[min(int(n * 0.99), n - 1)], 3),
                        "min_ms": round(s[0], 3),
                        "max_ms": round(s[-1], 3),
                    }

                return {
                    "session_id": session_id,
                    "sandbox_status": "ok",
                    "orchestrator_total_ms": round(total_ms, 3),
                    "claim_ms": round(claim_ms, 3),
                    "cold_start_ms": round(cold_start_ms, 3),
                    "rss_mb": rss_mb,
                    "navigate": _compute_stats(navigate_ms),
                    "evaluate": _compute_stats(evaluate_ms),
                    "fill": _compute_stats(fill_ms),
                    "click": _compute_stats(click_ms),
                    "screenshot": _compute_stats(screenshot_ms),
                    "interaction": _compute_stats(interaction_ms),
                }

            except Exception as e:
                total_ms = (time.time() - t_start) * 1000.0
                logger.exception("Chromium CDP session %d failed", session_id)
                return {
                    "session_id": session_id,
                    "orchestrator_total_ms": round(total_ms, 3),
                    "claim_ms": round(claim_ms, 3),
                    "error": f"{type(e).__name__}: {e}",
                }
            finally:
                if sandbox is not None:
                    try:
                        sb_client.delete_sandbox(
                            sandbox.claim_name, namespace=sandbox_namespace
                        )
                    except Exception:
                        logger.warning(
                            "Failed to delete sandbox for session %d",
                            session_id,
                            exc_info=True,
                        )

        # Fire concurrent sessions
        tasks = [_run_chromium_session_cdp(i) for i in range(req.concurrent_sessions)]
        session_results = await asyncio.gather(*tasks)

    # Separate successful vs failed
    successful = [r for r in session_results if "error" not in r]
    failed = [r for r in session_results if "error" in r]

    # Aggregate metrics
    aggregate = {}
    if successful:
        orch_times = sorted(r["orchestrator_total_ms"] for r in successful)
        aggregate.update(_percentile_stats(orch_times, "orchestrator_total"))

        claim_times = sorted(r["claim_ms"] for r in successful if "claim_ms" in r)
        if claim_times:
            aggregate.update(_percentile_stats(claim_times, "claim"))

        # Aggregate cold start and RSS
        cold_starts = sorted(
            r["cold_start_ms"] for r in successful if "cold_start_ms" in r
        )
        if cold_starts:
            aggregate["cold_start_mean_ms"] = round(
                sum(cold_starts) / len(cold_starts), 3
            )
            aggregate["cold_start_p95_ms"] = round(
                cold_starts[min(int(len(cold_starts) * 0.95), len(cold_starts) - 1)], 3
            )

        rss_vals = sorted(
            r["rss_mb"] for r in successful if r.get("rss_mb") is not None
        )
        if rss_vals:
            aggregate["rss_end_mb"] = round(sum(rss_vals) / len(rss_vals), 1)

        # Aggregate per-task-type interaction stats
        for metric_key in (
            "interaction",
            "navigate",
            "evaluate",
            "click",
            "fill",
            "screenshot",
        ):
            means = sorted(
                r[metric_key]["mean_ms"]
                for r in successful
                if isinstance(r.get(metric_key), dict) and "mean_ms" in r[metric_key]
            )
            p95s = sorted(
                r[metric_key]["p95_ms"]
                for r in successful
                if isinstance(r.get(metric_key), dict) and "p95_ms" in r[metric_key]
            )
            if means:
                aggregate[f"{metric_key}_mean_ms"] = round(sum(means) / len(means), 3)
            if p95s:
                aggregate[f"{metric_key}_p95_ms"] = round(
                    p95s[min(int(len(p95s) * 0.95), len(p95s) - 1)], 3
                )

    return {
        "concurrent_sessions": req.concurrent_sessions,
        "successful_sessions": len(successful),
        "failed_sessions": len(failed),
        "aggregate": aggregate,
        "sessions": session_results,
    }


@app.post("/run")
async def run_agent(req: RunRequest):
    """Raw agent interaction — send any prompt, get back the agent text."""
    try:
        output = await _run_agent(req.prompt)
        return {"response": output}
    except Exception as e:
        logger.exception("Agent run failed")
        raise HTTPException(status_code=500, detail=str(e))


# =========================================================================
# Entry point
# =========================================================================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
