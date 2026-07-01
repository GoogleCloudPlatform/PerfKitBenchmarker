"""GKE Performance Agent -- ADK agent definition.

This file runs INSIDE the GKE cluster as part of the adk-agent Deployment
(see gke_deploy_utils.py for the K8s manifest). It is NOT run from the
machine executing PKB. The ADK agent pod serves a FastAPI app (main.py)
that PKB calls via HTTP through a kubectl port-forward tunnel.

Execution flow:
  PKB (your laptop/CI) -> kubectl port-forward -> adk-agent pod -> this file
  -> GkeCodeExecutor -> SandboxClient -> gVisor sandbox pod
"""

"""GKE Performance Agent â ADK agent definition for sandbox benchmarking.

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
                                                -> V3GkeCodeExecutor._execute_in_sandbox()
                                                  -> SandboxClient.create_sandbox()
                                                  -> sandbox.files.write("script.py", code)
                                                  -> sandbox.commands.run("python3 script.py")
                                                  -> SandboxClient.delete_sandbox()

    The PKB machine communicates with this agent via HTTP (port-forwarded
    through kubectl or via a LoadBalancer/ClusterIP service).
"""

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

# Load the benchmark scripts
density_script_path = os.path.join(
    basedir, "../sandboxed_apps/python_test_app/benchmark_density.py"
)
try:
    with open(density_script_path, "r") as f:
        density_benchmark_code = f.read()
except Exception:
    density_benchmark_code = "import os; print(os.uname())"

payload_script_path = os.path.join(
    basedir, "../sandboxed_apps/python_test_app/benchmark_payload.py"
)
try:
    with open(payload_script_path, "r") as f:
        payload_benchmark_code = f.read()
except Exception:
    payload_benchmark_code = "import os; print(os.uname())"

qps_script_path = os.path.join(
    basedir, "../sandboxed_apps/python_test_app/benchmark_qps.py"
)
try:
    with open(qps_script_path, "r") as f:
        qps_benchmark_code = f.read()
except Exception:
    qps_benchmark_code = "import json; print(json.dumps({'sandbox_status': 'ok'}))"

# Keys that main.py sets in os.environ per-request.  We inject them into
# the script so they reach the sandbox pod.  If unset, the benchmark scripts
# use their own built-in defaults.
_DENSITY_ENV_KEYS = ["SAMPLE_COUNT", "SAMPLE_WARMUP"]
_PAYLOAD_ENV_KEYS = ["PAYLOAD_SIZE_MB", "PAYLOAD_ITERATIONS"]
_QPS_ENV_KEYS: list[str] = []  # QPS script needs no env config


def _build_benchmark_code() -> str:
    """Build the benchmark script with current env values injected.

    Selects the script based on BENCHMARK_MODE env var:
      - 'density'  → benchmark_density.py
      - 'payload'  → benchmark_payload.py
      - 'qps'      → benchmark_qps.py
    """
    mode = os.getenv("BENCHMARK_MODE", "density")

    if mode == "payload":
        env_keys = _PAYLOAD_ENV_KEYS
        script = payload_benchmark_code
    elif mode == "qps":
        env_keys = _QPS_ENV_KEYS
        script = qps_benchmark_code
    else:
        env_keys = _DENSITY_ENV_KEYS
        script = density_benchmark_code

    lines = ["import os"]
    for k in env_keys:
        v = os.getenv(k)
        if v is not None:
            lines.append(f"os.environ['{k}'] = '{v}'")
    return "\n".join(lines) + "\n\n" + script


class MockLlm(BaseLlm):
    model: str = "mock-model"

    async def generate_content_async(self, llm_request, stream=False):
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
_SANDBOX_POOL = ThreadPoolExecutor(max_workers=16)


class V3GkeCodeExecutor(GkeCodeExecutor):
    def _execute_in_sandbox(self, code: str) -> CodeExecutionResult:
        """Executes code using the v0.4.6 compatible SandboxClient."""
        from k8s_agent_sandbox.sandbox_client import SandboxClient
        from k8s_agent_sandbox.models import SandboxDirectConnectionConfig
        import logging
        import time

        logging.info("Executing via V3 SandboxClient (v0.4.6 compatible).")

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
        # v0.4.6 create_sandbox uses 'template' and 'namespace' arguments
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
            # v0.4.6 handles file I/O via the .files namespace
            t0 = time.time()
            upload_future = _SANDBOX_POOL.submit(sandbox.files.write, "script.py", code)
            upload_future.result()
            upload_ms = (time.time() - t0) * 1000.0

            # SANDBOX_EXEC_TIMEOUT_S is set per-request by main.py.
            # Default 60 s keeps density/snapshot runs tight; payload
            # sweeps raise it for large blobs.
            run_timeout = int(os.getenv("SANDBOX_EXEC_TIMEOUT_S", "60"))

            t0 = time.time()
            run_future = _SANDBOX_POOL.submit(
                sandbox.commands.run, "python3 script.py", timeout=run_timeout
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


gke_executor = V3GkeCodeExecutor(
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
