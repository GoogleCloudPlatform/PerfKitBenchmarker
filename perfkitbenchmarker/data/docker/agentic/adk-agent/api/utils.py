import asyncio
import json
import logging
import os
import re
import time
from typing import Optional

from google.adk.sessions import InMemorySessionService
from google.adk.artifacts import InMemoryArtifactService
from google.adk.runners import Runner
from google.genai import types

from k8s_agent_sandbox.sandbox_client import SandboxClient
from k8s_agent_sandbox.models import SandboxDirectConnectionConfig

from performance_agent import agent

logger = logging.getLogger(__name__)

APP_NAME = "performance_agent_app"
USER_ID = "benchmark_user"

benchmark_lock = asyncio.Lock()

def make_sandbox_client() -> SandboxClient:
    router_url = os.getenv("SANDBOX_ROUTER_URL")
    if router_url:
        return SandboxClient(connection_config=SandboxDirectConnectionConfig(api_url=router_url))
    return SandboxClient()

def calculate_percentile(sorted_values: list[float], fraction: float) -> float:
    if not sorted_values:
        return 0.0
    idx = fraction * (len(sorted_values) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_values) - 1)
    weight = idx - lo
    return sorted_values[lo] * (1 - weight) + sorted_values[hi] * weight

def percentile_stats(sorted_values: list, prefix: str) -> dict:
    n = len(sorted_values)
    if n == 0:
        return {}
    return {
        f"{prefix}_mean_ms": round(sum(sorted_values) / n, 6),
        f"{prefix}_p50_ms": round(calculate_percentile(sorted_values, 0.50), 6),
        f"{prefix}_p95_ms": round(calculate_percentile(sorted_values, 0.95), 6),
        f"{prefix}_p99_ms": round(calculate_percentile(sorted_values, 0.99), 6),
        f"{prefix}_min_ms": round(sorted_values[0], 6),
        f"{prefix}_max_ms": round(sorted_values[-1], 6),
    }

_JSON_RE = re.compile(r"\{[^{}]*\}", re.DOTALL)

def parse_sandbox_json(raw_output: str) -> Optional[dict]:
    matches = _JSON_RE.findall(raw_output)
    for candidate in reversed(matches):
        try:
            obj = json.loads(candidate)
            if any(k.startswith("sandbox_") for k in obj):
                return obj
        except json.JSONDecodeError:
            continue
    return None

async def run_agent(prompt: str) -> str:
    session_service = InMemorySessionService()
    artifact_service = InMemoryArtifactService()
    session = await session_service.create_session(app_name=APP_NAME, user_id=USER_ID, state={})
    runner = Runner(agent=agent.root_agent, app_name=APP_NAME, session_service=session_service, artifact_service=artifact_service)
    content = types.Content(role="user", parts=[types.Part(text=prompt)])
    final_response = ""
    code_execution_output = ""
    async with runner:
        async for event in runner.run_async(user_id=USER_ID, session_id=session.id, new_message=content):
            if event.content and event.content.parts:
                for part in event.content.parts:
                    cer = getattr(part, "code_execution_result", None) or getattr(part, "codeExecutionResult", None)
                    if cer:
                        code_execution_output = getattr(cer, "output", "") or ""
            if event.is_final_response() and event.content and event.content.parts:
                final_response = event.content.parts[0].text
    await session_service.delete_session(app_name=APP_NAME, user_id=USER_ID, session_id=session.id)
    return code_execution_output if code_execution_output else final_response

async def run_single_session(session_id: int, prompt: str) -> dict:
    orchestrator_start = time.perf_counter()
    logger.info("SESSION_START: session_id=%d start_ts=%.3f", session_id, time.time())
    try:
        raw_output = await run_agent(prompt)
    except Exception as e:
        return {"session_id": session_id, "error": str(e)}
    orchestrator_elapsed_ms = round((time.perf_counter() - orchestrator_start) * 1000, 6)
    logger.info("SESSION_END: session_id=%d elapsed_ms=%.3f", session_id, orchestrator_elapsed_ms)
    sandbox_metrics = parse_sandbox_json(raw_output) or {}
    return {"session_id": session_id, "orchestrator_total_ms": orchestrator_elapsed_ms, "raw_output": raw_output, **sandbox_metrics}
