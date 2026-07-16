"""Shared utilities for GKE Agent Sandbox benchmarks.

Provides helpers for agent API interaction, kubectl commands, warm pool
management, and sample construction used by all GKE agent benchmark
definitions.
"""

from __future__ import annotations


import json
import logging
import subprocess
import time
import urllib.request
import urllib.error

from absl import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources.container_service import kubectl

FLAGS = flags.FLAGS

# Module-level benchmark_spec reference for metadata derivation.
# Set by each benchmark's Run() via set_benchmark_spec().
_current_benchmark_spec = None


# ---------------------------------------------------------------------------
# Shared flags (registered once; importable by benchmark modules)
# ---------------------------------------------------------------------------

flags.DEFINE_string(
    "k8s_agentic_namespace",
    "agentic",
    "Kubernetes namespace where the agentic workloads are deployed.",
)

flags.DEFINE_bool(
    "k8s_agentic_gvisor",
    True,
    "Whether the sandbox node pool uses gVisor. Recorded in sample metadata.",
)

flags.DEFINE_string(
    "k8s_agentic_benchmark_note",
    "",
    "Arbitrary note string attached to every sample for tagging runs.",
)

flags.DEFINE_string(
    "k8s_agentic_agent_api_url",
    "http://localhost:8080",
    "Base URL of the ADK Agent API.",
)

flags.DEFINE_integer(
    "k8s_agentic_agent_api_timeout",
    600,
    "HTTP timeout in seconds for agent API benchmark calls.",
)


# ---------------------------------------------------------------------------
# Agent API helpers
# ---------------------------------------------------------------------------


def GetAgentApiUrl() -> str:
    """Return the base URL of the ADK agent API service.
    
    Auto-derives the URL from the port-forward local port when the
    API URL is at its default value but the port has been changed.
    This allows concurrent runs with just --k8s_agentic_portforward_local_port.
    """
    url = FLAGS.k8s_agentic_agent_api_url
    port = FLAGS.k8s_agentic_portforward_local_port
    if url == "http://localhost:8080" and port != 8080:
        url = f"http://localhost:{port}"
    return url.rstrip("/")


def CheckAgentHealthz(api_url: str | None = None, required: bool = True) -> None:
    """Verify the agent API is reachable via /healthz.

    Args:
        api_url: Base URL to check. Defaults to FLAGS.k8s_agentic_agent_api_url.
        required: If True (default), raise on failure. If False, log warning.
    """
    if api_url is None:
        api_url = GetAgentApiUrl()
    try:
        req = urllib.request.Request(f"{api_url}/healthz")
        with urllib.request.urlopen(req, timeout=15) as resp:
            logging.info("Agent healthz: %s", resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError) as e:
        msg = (
            f"Agent API is not reachable at {api_url}/healthz: {e}\n"
            "Hint: ensure kubectl port-forward is running "
            "(kubectl port-forward svc/adk-agent -n <ns> 8080:80)."
        )
        if required:
            raise RuntimeError(msg)
        else:
            logging.warning("Health check deferred (non-fatal): %s", msg)


def CallAgentApi(endpoint: str, payload: dict, timeout: int | None = None) -> dict:
    """POST JSON to an agent API endpoint and return the parsed response."""
    if timeout is None:
        timeout = FLAGS.k8s_agentic_agent_api_timeout
    base_url = GetAgentApiUrl()
    url = f"{base_url}{endpoint}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    logging.info("POST %s  payload=%s  timeout=%ds", url, payload, timeout)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Agent API returned HTTP {e.code}: {body[:500]}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Cannot reach agent API at {url}: {e.reason}")
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        raise RuntimeError(f"Agent API returned non-JSON response:\n{body[:500]}")


# ---------------------------------------------------------------------------
# kubectl helpers
# ---------------------------------------------------------------------------


def RunKubectl(args: list[str], timeout: int = 120, raise_on_failure: bool = True) -> tuple[str, str, int]:
    """Run a kubectl command and return (stdout, stderr, retcode).

    Delegates to PKB's native kubectl module which handles kubeconfig
    and retries for transient connection errors automatically.
    """
    return kubectl.RunKubectlCommand(
        list(args),
        timeout=timeout,
        raise_on_failure=raise_on_failure,
    )


def CountPods(namespace: str, label: str, phase: str | None = None) -> int:
    """Count pods matching label (and optionally phase)."""
    cmd = ["get", "pods", "-n", namespace, "-l", label, "-o", "name"]
    if phase:
        cmd += [f"--field-selector=status.phase={phase}"]
    stdout, _, rc = RunKubectl(cmd, raise_on_failure=False)
    if not stdout or not stdout.strip():
        return 0
    return len(stdout.strip().splitlines())


def PatchWarmPool(namespace: str, warmpool_name: str, replicas: int, label: str, wait_timeout: int = 180) -> bool:
    """Patch SandboxWarmPool replicas and wait for pods to be ready."""
    logging.info("Patching %s replicas -> %d", warmpool_name, replicas)
    patch_json = json.dumps({"spec": {"replicas": replicas}})
    RunKubectl([
        "patch", "sandboxwarmpool", warmpool_name,
        "-n", namespace, "--type=merge", f"-p={patch_json}",
    ])
    if replicas == 0:
        return True
    deadline = time.time() + wait_timeout
    while time.time() < deadline:
        running = CountPods(namespace, label, phase="Running")
        logging.info("%d/%d warm pool pods Running", running, replicas)
        if running >= replicas:
            return True
        time.sleep(3)
    logging.warning("Timed out waiting for %d warm pool pods", replicas)
    return False


def DrainWarmPool(namespace: str, warmpool_name: str, label: str, timeout: int = 120) -> bool:
    """Scale warm pool to 0 and wait for all pods to terminate."""
    logging.info("Draining warm pool %s to 0", warmpool_name)
    patch_json = json.dumps({"spec": {"replicas": 0}})
    RunKubectl([
        "patch", "sandboxwarmpool", warmpool_name,
        "-n", namespace, "--type=merge", f"-p={patch_json}",
    ], raise_on_failure=False)

    # Delete lingering SandboxClaims that may prevent pod termination
    RunKubectl([
        "delete", "sandboxclaims", "--all",
        "-n", namespace, "--ignore-not-found=true",
    ], timeout=60, raise_on_failure=False)

    deadline = time.time() + timeout
    while time.time() < deadline:
        remaining = CountPods(namespace, label)
        if remaining == 0:
            logging.info("Warm pool drained successfully")
            return True
        logging.info("Draining... %d pods remaining", remaining)
        time.sleep(2)
    logging.warning("Drain timed out, %d pods still present",
                    CountPods(namespace, label))
    return False


def set_benchmark_spec(benchmark_spec: object) -> None:
    """Store benchmark_spec for metadata derivation (called by Run())."""
    global _current_benchmark_spec
    _current_benchmark_spec = benchmark_spec




# ---------------------------------------------------------------------------
# Sample construction
# ---------------------------------------------------------------------------


def BuildMetadata(namespace: str, extra: dict | None = None) -> dict[str, object]:
    """Construct the common metadata dict for all samples."""
    metadata = {
        "namespace": namespace,
        "gvisor": FLAGS.k8s_agentic_gvisor,
    }
    # Derive machine_type from benchmark_spec (set via set_benchmark_spec)
    machine_type = None
    if _current_benchmark_spec:
        cluster = getattr(_current_benchmark_spec, 'container_cluster', None)
        if cluster:
            # Prefer sandbox nodepool machine_type over default pool
            nodepools = getattr(cluster, 'nodepools', None)
            if nodepools and isinstance(nodepools, dict):
                sandbox_pool = nodepools.get('sandbox')
                if sandbox_pool and hasattr(sandbox_pool, 'vm_spec'):
                    machine_type = getattr(sandbox_pool.vm_spec, 'machine_type', None)
            if not machine_type and hasattr(cluster, 'vm_spec'):
                machine_type = getattr(cluster.vm_spec, 'machine_type', None)
    if machine_type:
        metadata["machine_type"] = machine_type
    if FLAGS.k8s_agentic_benchmark_note:
        metadata["note"] = FLAGS.k8s_agentic_benchmark_note
    if extra:
        metadata.update(extra)
    return metadata


def MakeSample(metric: str, value: float, unit: str, namespace: str, extra_metadata: dict | None = None) -> sample.Sample:
    """Create a single sample.Sample with standard metadata."""
    return sample.Sample(
        metric=metric,
        value=value,
        unit=unit,
        metadata=BuildMetadata(namespace, extra_metadata),
    )


# ---------------------------------------------------------------------------
# Port-forward flags
# ---------------------------------------------------------------------------

flags.DEFINE_bool(
    "k8s_agentic_auto_portforward",
    True,
    "Automatically manage kubectl port-forward to the agent service.",
)

flags.DEFINE_integer(
    "k8s_agentic_portforward_local_port",
    8080,
    "Local port for kubectl port-forward.",
)

flags.DEFINE_integer(
    "k8s_agentic_portforward_remote_port",
    80,
    "Remote service port for kubectl port-forward.",
)

flags.DEFINE_string(
    "k8s_agentic_portforward_service",
    "svc/adk-agent",
    "Kubernetes service to port-forward to.",
)

flags.DEFINE_float(
    "k8s_agentic_portforward_reconnect_delay",
    1.0,
    "Seconds to wait before reconnecting after port-forward drops.",
)

flags.DEFINE_float(
    "k8s_agentic_portforward_health_timeout",
    30.0,
    "Seconds to wait for agent health check after starting port-forward.",
)


# ---------------------------------------------------------------------------
# Port-forward manager
# ---------------------------------------------------------------------------

import atexit
import os as _os
import signal
import threading


_PID_FILE = "/tmp/pkb_portforward.pid"


class _PortForwardManager:
    """Manages a kubectl port-forward subprocess with auto-reconnect.

    Mimics the shell pattern:
        while true; do
          kubectl port-forward svc/adk-agent -n agentic 8080:80
          echo "Reconnecting..."
          sleep 1
        done

    Thread-safe. Idempotent start/stop. Cleans up orphans via PID file.
    """

    def __init__(self) -> None:
        self._proc = None
        self._thread = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._started = False

    @property
    def is_running(self) -> bool:
        return self._started and not self._stop_event.is_set()

    def start(self) -> None:
        """Start the port-forward loop (idempotent)."""
        with self._lock:
            if self._started and not self._stop_event.is_set():
                if self._proc and self._proc.poll() is None:
                    return
                return

            self._kill_orphan()
            self._stop_event.clear()
            self._started = True
            self._thread = threading.Thread(
                target=self._loop, daemon=True, name="pkb-portforward"
            )
            self._thread.start()

    def stop(self) -> None:
        """Stop the port-forward loop and kill the subprocess."""
        with self._lock:
            if not self._started:
                return
            self._stop_event.set()
            self._kill_proc()
            self._started = False
            self._cleanup_pid_file()

    def _loop(self) -> None:
        """Background reconnect loop."""
        ns = FLAGS.k8s_agentic_namespace
        svc = FLAGS.k8s_agentic_portforward_service
        local_port = FLAGS.k8s_agentic_portforward_local_port
        remote_port = FLAGS.k8s_agentic_portforward_remote_port
        delay = FLAGS.k8s_agentic_portforward_reconnect_delay

        cmd = ["kubectl"]
        if FLAGS.kubeconfig:
            cmd += ["--kubeconfig", FLAGS.kubeconfig]
        cmd += [
            "port-forward", svc,
            "-n", ns,
            f"{local_port}:{remote_port}",
        ]

        while not self._stop_event.is_set():
            logging.info("Starting port-forward: %s", " ".join(cmd))
            try:
                self._proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                self._write_pid_file(self._proc.pid)

                while not self._stop_event.is_set():
                    retcode = self._proc.poll()
                    if retcode is not None:
                        break
                    self._stop_event.wait(timeout=0.5)

            except Exception as e:
                logging.warning("Port-forward error: %s", e)

            if not self._stop_event.is_set():
                logging.info(
                    "Port-forward disconnected. Reconnecting in %.1fs...", delay
                )
                self._stop_event.wait(timeout=delay)

    def _kill_proc(self) -> None:
        """Kill the current subprocess if alive."""
        if self._proc and self._proc.poll() is None:
            try:
                self._proc.terminate()
                self._proc.wait(timeout=5)
            except Exception:
                try:
                    self._proc.kill()
                except Exception:
                    pass
        self._proc = None

    def _write_pid_file(self, pid: int) -> None:
        """Write PID to file for orphan detection."""
        try:
            with open(_PID_FILE, "w") as f:
                f.write(str(pid))
        except Exception:
            pass

    def _cleanup_pid_file(self) -> None:
        """Remove PID file."""
        try:
            _os.unlink(_PID_FILE)
        except OSError:
            pass

    def _kill_orphan(self) -> None:
        """Kill a port-forward process left by a previous PKB run."""
        try:
            if _os.path.exists(_PID_FILE):
                with open(_PID_FILE, "r") as f:
                    pid = int(f.read().strip())
                logging.info("Killing orphan port-forward (PID %d)", pid)
                _os.kill(pid, signal.SIGTERM)
                time.sleep(0.5)
                try:
                    _os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
                self._cleanup_pid_file()
        except (OSError, ValueError):
            self._cleanup_pid_file()

        local_port = FLAGS.k8s_agentic_portforward_local_port
        try:
            result = subprocess.run(
                ["lsof", "-ti", f":{local_port}"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                for pid_str in result.stdout.strip().split():
                    try:
                        pid = int(pid_str)
                        _os.kill(pid, signal.SIGTERM)
                        logging.info("Killed process %d on port %d", pid, local_port)
                    except (OSError, ValueError):
                        pass
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass


# Singleton instance
_port_forward_manager = _PortForwardManager()

# Ensure cleanup on interpreter exit
atexit.register(_port_forward_manager.stop)


def EnsurePortForward() -> None:
    """Start port-forward if auto_portforward is enabled (idempotent).

    Blocks until the agent health check passes or timeout is reached.
    Safe to call multiple times - only starts one background loop.
    """
    if not FLAGS.k8s_agentic_auto_portforward:
        logging.info("Auto port-forward disabled (--k8s_agentic_auto_portforward=false)")
        return

    _port_forward_manager.start()

    timeout = FLAGS.k8s_agentic_portforward_health_timeout
    deadline = time.time() + timeout
    api_url = GetAgentApiUrl()

    while time.time() < deadline:
        try:
            req = urllib.request.Request(f"{api_url}/healthz")
            with urllib.request.urlopen(req, timeout=3) as resp:
                logging.info("Port-forward healthy: %s", resp.read().decode())
                return
        except Exception:
            time.sleep(1)

    logging.warning(
        "Port-forward health check did not pass within %.0fs. "
        "Continuing anyway (Run() will fail if agent is unreachable).",
        timeout,
    )


def StopPortForward() -> None:
    """Stop the port-forward subprocess and clean up."""
    _port_forward_manager.stop()
    logging.info("Port-forward stopped.")
