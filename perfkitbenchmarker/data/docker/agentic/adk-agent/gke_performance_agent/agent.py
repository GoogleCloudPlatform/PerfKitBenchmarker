"""GKE Performance Agent -- ADK agent definition for sandbox benchmarking.

EXECUTION CONTEXT:
    This file runs INSIDE the GKE cluster, NOT on the PKB orchestrator machine.
    It is packaged into a container image (see ../Dockerfile) and deployed as
    the 'adk-agent' Deployment in the benchmark namespace.

    Execution flow:
      PKB machine                          GKE Cluster
      ----------                           -----------
      benchmark.Run()
        -> CallAgentApi("/benchmark/...")   -> main.py (FastAPI)
                                              -> Runner(agent=root_agent)
                                                -> MockLlm yields code
                                                -> BenchmarkGkeCodeExecutor._execute_in_sandbox()
                                                  -> SandboxClient.create_sandbox()
                                                  -> sandbox.files.write("script.py", code)
                                                  -> sandbox.commands.run("python3 script.py")
                                                  -> SandboxClient.delete_sandbox()

    The PKB machine communicates with this agent via HTTP (port-forwarded
    through kubectl or via a LoadBalancer/ClusterIP service).
"""

from __future__ import annotations


from google.adk.agents import LlmAgent
from google.adk.code_executors import GkeCodeExecutor
from google.adk.code_executors.code_execution_utils import CodeExecutionResult
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_response import LlmResponse
from google.genai import types
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from google.adk.apps import App
import logging
import os
import time

from k8s_agent_sandbox.sandbox_client import SandboxClient
from k8s_agent_sandbox.models import SandboxDirectConnectionConfig

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO)

# =========================================================================
# 1. Environment and Configuration
# =========================================================================

basedir = os.path.abspath(os.path.dirname(__file__))
agent_dir = os.path.join(basedir, "..")

# Load generated.env (rendered by gke_image_build_utils._GenerateEnvFile from PKB flags).
# In GKE, K8s manifest env vars take precedence.
load_dotenv(os.path.join(agent_dir, "generated.env"))

# =========================================================================
# 2. Mock LLM Definition (Inheriting from BaseLlm for Pydantic)
# =========================================================================

# ---------------------------------------------------------------------------
# Benchmark script loading
# ---------------------------------------------------------------------------
# Scripts are loaded at module import time (container startup). If any script
# is missing, the container will fail to start and enter CrashLoopBackOff —
# this is intentional. A missing script means the Docker image was built
# incorrectly, and we want immediate visibility rather than silently running
# a no-op that produces garbage metrics.

_SCRIPTS_DIR = os.path.join(basedir, "..", "sandboxed_apps", "python_test_app")


def _load_script(filename: str) -> str:
    """Load a benchmark script from the sandboxed_apps directory."""
    path = os.path.join(_SCRIPTS_DIR, filename)
    with open(path, "r") as f:
        return f.read()


density_benchmark_code = _load_script("benchmark_density.py")
payload_benchmark_code = _load_script("benchmark_payload.py")
qps_benchmark_code = _load_script("benchmark_qps.py")

# Keys that main.py sets in os.environ per-request.  We inject them into
# the script so they reach the sandbox pod.  If unset, the benchmark scripts
# use their own built-in defaults.
_DENSITY_ENV_KEYS = ["SAMPLE_COUNT", "SAMPLE_WARMUP"]
_PAYLOAD_ENV_KEYS = ["PAYLOAD_SIZE_MB", "PAYLOAD_ITERATIONS"]
_QPS_ENV_KEYS: list[str] = []  # QPS script needs no env config


def _build_benchmark_code() -> str:
    """Select the benchmark script based on BENCHMARK_MODE env var."""
    mode = os.getenv("BENCHMARK_MODE", "density")
    if mode == "payload":
        return payload_benchmark_code
    elif mode == "qps":
        return qps_benchmark_code
    return density_benchmark_code


def _build_env_prefix() -> str:
    """Build shell env var prefix for the sandbox command.

    Returns a string like "KEY1=val1 KEY2=val2" prepended to the
    python3 command so env vars are set in the sandbox shell.
    """
    mode = os.getenv("BENCHMARK_MODE", "density")
    if mode == "payload":
        env_keys = _PAYLOAD_ENV_KEYS
    elif mode == "qps":
        env_keys = _QPS_ENV_KEYS
    else:
        env_keys = _DENSITY_ENV_KEYS
    parts = []
    for k in env_keys:
        v = os.getenv(k)
        if v is not None:
            parts.append(f"{k}={v}")
    return " ".join(parts)



class MockLlm(BaseLlm):
    model: str = "mock-model"

    async def generate_content_async(self, llm_request: object, stream: bool = False):
        """Mock the ADK response loop.

        BaseLlm.generate_content_async is an AsyncGenerator — it must YIELD
        LlmResponse objects, never return them.
        """
        # ADK appends the code execution result to the conversation
        # history before calling the LLM again.  If the history has
        # grown beyond the initial user prompt, code has already
        # executed — return plain text to stop the loop.
        has_execution_result = len(llm_request.contents) > 1

        if has_execution_result:
            part = types.Part(text="Execution Complete")
        else:
            # Create an ADK-compliant result with executable code.
            # Build at request time so SAMPLE_COUNT/SAMPLE_WARMUP reflect
            # the current os.environ values set by main.py per-request.
            part = types.Part(
                executable_code=types.ExecutableCode(
                    language="PYTHON", code=_build_benchmark_code()
                )
            )

        content = types.Content(role="model", parts=[part])
        response = LlmResponse(content=content, partial=False)

        # Yield exactly one final response (both streaming and non-streaming)
        yield response


# =========================================================================
# 3. Agent Initialization
# =========================================================================


# Module-level thread pool for sandbox I/O operations.
# Initialized once at import time to avoid thread-safety issues
# with lazy initialization inside _execute_in_sandbox().
def _sandbox_io_workers() -> int:
    """Compute worker count for sandbox I/O thread pool.

    Uses the same 2 * cpu_count heuristic as main.py's
    _compute_thread_count(), capped between 2 and 64.  On
    c4-standard-8 (8 vCPUs) this produces 16 — matching the
    original hardcoded value.

    Override via kubectl set env or Deployment patch:
        kubectl set env deploy/adk-agent SANDBOX_IO_WORKERS=32
    """
    env_val = os.getenv("SANDBOX_IO_WORKERS")
    if env_val:
        return int(env_val)
    return max(2, min(64, 2 * (os.cpu_count() or 1)))


_SANDBOX_POOL = ThreadPoolExecutor(max_workers=_sandbox_io_workers())


class BenchmarkGkeCodeExecutor(GkeCodeExecutor):
    def _execute_in_sandbox(self, code: str) -> CodeExecutionResult:
        """Executes code in a sandbox with benchmark instrumentation."""
        logging.info("Executing in benchmark-instrumented sandbox.")

        # _SANDBOX_POOL is initialized at module level (thread-safe).

        # Use DirectConnection when SANDBOX_ROUTER_URL is set (in-cluster),
        # otherwise fall back to kubectl port-forward (dev mode).
        router_url = os.getenv("SANDBOX_ROUTER_URL")
        if router_url:
            client = SandboxClient(
                connection_config=SandboxDirectConnectionConfig(api_url=router_url)
            )
        else:
            client = SandboxClient()
        # create_sandbox uses 'template' and 'namespace' arguments
        create_ms = upload_ms = run_ms = delete_ms = 0.0
        sandbox = None
        # Time sandbox creation
        t0 = time.time()
        create_future = _SANDBOX_POOL.submit(
            client.create_sandbox,
            template=self.sandbox_template,
            namespace=self.namespace,
        )
        sandbox = create_future.result()
        create_ms = (time.time() - t0) * 1000.0
        try:
            # File I/O via the .files namespace
            t0 = time.time()
            upload_future = _SANDBOX_POOL.submit(sandbox.files.write, "script.py", code)
            upload_future.result()
            upload_ms = (time.time() - t0) * 1000.0

            # SANDBOX_EXEC_TIMEOUT_S is set per-request by main.py.
            # Default 60 s keeps density/snapshot runs tight; payload
            # sweeps raise it for large blobs.
            run_timeout = int(os.getenv("SANDBOX_EXEC_TIMEOUT_S", "60"))
            env_prefix = _build_env_prefix()
            # Wrap in /bin/sh -c because sandbox.commands.run() uses
            # exec() directly -- env var prefix needs shell interpretation.
            if env_prefix:
                run_cmd = f"/bin/sh -c '{env_prefix} python3 script.py'"
            else:
                run_cmd = "python3 script.py"

            t0 = time.time()
            run_future = _SANDBOX_POOL.submit(
                sandbox.commands.run, run_cmd, timeout=run_timeout
            )
            result = run_future.result()
            run_ms = (time.time() - t0) * 1000.0

            # ADK's build_code_execution_result_part discards stdout when
            # stderr is non-empty (OUTCOME_FAILED path).  Sandbox scripts
            # produce benign stderr (C-extension reimport noise, gVisor
            # warnings) that would cause all sandbox_* metrics to vanish.
            # Log stderr for debugging, then clear it so ADK passes
            # stdout through.
            if result.stderr:
                logging.warning("Sandbox stderr (ignored): %s", result.stderr[:500])

            logging.info(
                "SANDBOX_TIMINGS: create_ms=%.3f upload_ms=%.3f run_ms=%.3f",
                create_ms,
                upload_ms,
                run_ms,
            )
            return CodeExecutionResult(stdout=result.stdout, stderr="")
        finally:
            # Always cleanup the claim
            t0 = time.time()
            if sandbox is not None:
                delete_future = _SANDBOX_POOL.submit(
                    client.delete_sandbox, sandbox.claim_name, namespace=self.namespace
                )
                delete_future.result()
            delete_ms = (time.time() - t0) * 1000.0
            logging.info("SANDBOX_TIMINGS_DELETE: delete_ms=%.3f", delete_ms)


gke_executor = BenchmarkGkeCodeExecutor(
    cluster_name=os.getenv("CLUSTER_NAME"),
    location=os.getenv("GOOGLE_CLOUD_LOCATION"),
    namespace=os.getenv("AGENTIC_NAMESPACE"),
    executor_type="sandbox",
    sandbox_template="python-sandbox-template",
)

gke_performance_agent = LlmAgent(
    name="gke_performance_agent",  # Must be a valid identifier (no dashes)
    model=MockLlm(model="mock-model"),
    code_executor=gke_executor,
)

root_agent = gke_performance_agent

app = App(
    name=root_agent.name,
    root_agent=root_agent,
    # enable_tracing=True,
)
