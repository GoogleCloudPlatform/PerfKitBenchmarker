"""Shared utilities for GKE Agent Sandbox benchmarks.

Provides helpers for agent API interaction, kubectl commands, warm pool
management, and sample construction used by all GKE agent benchmark
definitions.
"""

import json
import logging
import subprocess
import time
import urllib.request
import urllib.error

from absl import flags
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS

# ---------------------------------------------------------------------------
# Shared flags (registered once; importable by benchmark modules)
# ---------------------------------------------------------------------------

flags.DEFINE_string(
    "gke_namespace",
    "agentic",
    "Kubernetes namespace where the agentic workloads are deployed.",
)

flags.DEFINE_string(
    "gke_machine_type",
    "",
    "Machine type of the sandbox node pool. Recorded in sample metadata.",
)

flags.DEFINE_string(
    "gke_kubeconfig",
    "",
    "Path to a kubeconfig file. If empty, the system default is used.",
)

flags.DEFINE_bool(
    "gke_gvisor",
    True,
    "Whether the sandbox node pool uses gVisor. Recorded in sample metadata.",
)

flags.DEFINE_string(
    "gke_note",
    "",
    "Arbitrary note string attached to every sample for tagging runs.",
)

flags.DEFINE_string(
    "gke_api_url",
    "http://localhost:8080",
    "Base URL of the ADK Agent API.",
)

flags.DEFINE_integer(
    "gke_api_timeout",
    600,
    "HTTP timeout in seconds for agent API benchmark calls.",
)


# ---------------------------------------------------------------------------
# Agent API helpers
# ---------------------------------------------------------------------------


def GetAgentApiUrl():
    """Return the base URL of the ADK agent API service."""
    return FLAGS.gke_api_url.rstrip("/")


def CheckAgentHealthz(api_url=None, required=True):
    """Verify the agent API is reachable via /healthz.

    Args:
        api_url: Base URL to check. Defaults to FLAGS.gke_api_url.
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


def CallAgentApi(endpoint, payload, timeout=None):
    """POST JSON to an agent API endpoint and return the parsed response."""
    if timeout is None:
        timeout = FLAGS.gke_api_timeout
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


def _KubectlCmd(args):
    """Build a kubectl command list, optionally injecting --kubeconfig."""
    cmd = ["kubectl"]
    if FLAGS.gke_kubeconfig:
        cmd += ["--kubeconfig", FLAGS.gke_kubeconfig]
    return cmd + list(args)


def RunKubectl(args, timeout=120, raise_on_failure=True):
    """Run a kubectl command and return (stdout, stderr, retcode)."""
    cmd = _KubectlCmd(args)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if raise_on_failure and proc.returncode != 0:
        raise RuntimeError(
            f"kubectl failed (rc={proc.returncode}): {proc.stderr}"
        )
    return proc.stdout, proc.stderr, proc.returncode


def CountPods(namespace, label, phase=None):
    """Count pods matching label (and optionally phase)."""
    cmd = ["get", "pods", "-n", namespace, "-l", label, "-o", "name"]
    if phase:
        cmd += [f"--field-selector=status.phase={phase}"]
    stdout, _, rc = RunKubectl(cmd, raise_on_failure=False)
    if rc != 0 or not stdout:
        return 0
    return len(stdout.strip().splitlines())


def PatchWarmPool(namespace, warmpool_name, replicas, label, wait_timeout=180):
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


def DrainWarmPool(namespace, warmpool_name, label, timeout=120):
    """Scale warm pool to 0 and wait for all pods to terminate."""
    logging.info("Draining warm pool %s to 0", warmpool_name)
    patch_json = json.dumps({"spec": {"replicas": 0}})
    RunKubectl([
        "patch", "sandboxwarmpool", warmpool_name,
        "-n", namespace, "--type=merge", f"-p={patch_json}",
    ], raise_on_failure=False)
    deadline = time.time() + timeout
    while time.time() < deadline:
        remaining = CountPods(namespace, label)
        if remaining == 0:
            logging.info("Warm pool drained successfully")
            return True
        logging.info("Draining... %d pods remaining", remaining)
        time.sleep(3)
    logging.warning("Drain timed out, %d pods still present",
                    CountPods(namespace, label))
    return False


# ---------------------------------------------------------------------------
# Sample construction
# ---------------------------------------------------------------------------


def BuildMetadata(namespace, extra=None):
    """Construct the common metadata dict for all samples."""
    metadata = {
        "namespace": namespace,
        "gvisor": FLAGS.gke_gvisor,
    }
    if FLAGS.gke_machine_type:
        metadata["machine_type"] = FLAGS.gke_machine_type
    if FLAGS.gke_note:
        metadata["note"] = FLAGS.gke_note
    if extra:
        metadata.update(extra)
    return metadata


def MakeSample(metric, value, unit, namespace, extra_metadata=None):
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
    "gke_auto_portforward",
    True,
    "Automatically manage kubectl port-forward to the agent service.",
)

flags.DEFINE_integer(
    "gke_portforward_local_port",
    8080,
    "Local port for kubectl port-forward.",
)

flags.DEFINE_integer(
    "gke_portforward_remote_port",
    80,
    "Remote service port for kubectl port-forward.",
)

flags.DEFINE_string(
    "gke_portforward_service",
    "svc/adk-agent",
    "Kubernetes service to port-forward to.",
)

flags.DEFINE_float(
    "gke_portforward_reconnect_delay",
    1.0,
    "Seconds to wait before reconnecting after port-forward drops.",
)

flags.DEFINE_float(
    "gke_portforward_health_timeout",
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

    def __init__(self):
        self._proc = None
        self._thread = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._started = False

    @property
    def is_running(self):
        return self._started and not self._stop_event.is_set()

    def start(self):
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

    def stop(self):
        """Stop the port-forward loop and kill the subprocess."""
        with self._lock:
            if not self._started:
                return
            self._stop_event.set()
            self._kill_proc()
            self._started = False
            self._cleanup_pid_file()

    def _loop(self):
        """Background reconnect loop."""
        ns = FLAGS.gke_namespace
        svc = FLAGS.gke_portforward_service
        local_port = FLAGS.gke_portforward_local_port
        remote_port = FLAGS.gke_portforward_remote_port
        delay = FLAGS.gke_portforward_reconnect_delay

        cmd = ["kubectl"]
        if FLAGS.gke_kubeconfig:
            cmd += ["--kubeconfig", FLAGS.gke_kubeconfig]
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

    def _kill_proc(self):
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

    def _write_pid_file(self, pid):
        """Write PID to file for orphan detection."""
        try:
            with open(_PID_FILE, "w") as f:
                f.write(str(pid))
        except Exception:
            pass

    def _cleanup_pid_file(self):
        """Remove PID file."""
        try:
            _os.unlink(_PID_FILE)
        except OSError:
            pass

    def _kill_orphan(self):
        """Kill a port-forward process left by a previous PKB run."""
        try:
            if _os.path.exists(_PID_FILE):
                with open(_PID_FILE, "r") as f:
                    pid = int(f.read().strip())
                logging.info("Killing orphan port-forward (PID %d)", pid)
                _os.kill(pid, signal.SIGTERM)
                import time as _time
                _time.sleep(0.5)
                try:
                    _os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
                self._cleanup_pid_file()
        except (OSError, ValueError):
            self._cleanup_pid_file()

        local_port = FLAGS.gke_portforward_local_port
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


def EnsurePortForward():
    """Start port-forward if auto_portforward is enabled (idempotent).

    Blocks until the agent health check passes or timeout is reached.
    Safe to call multiple times - only starts one background loop.
    """
    if not FLAGS.gke_auto_portforward:
        logging.info("Auto port-forward disabled (--gke_auto_portforward=false)")
        return

    _port_forward_manager.start()

    import time as _time
    timeout = FLAGS.gke_portforward_health_timeout
    deadline = _time.time() + timeout
    api_url = GetAgentApiUrl()

    while _time.time() < deadline:
        try:
            req = urllib.request.Request(f"{api_url}/healthz")
            with urllib.request.urlopen(req, timeout=3) as resp:
                logging.info("Port-forward healthy: %s", resp.read().decode())
                return
        except Exception:
            _time.sleep(1)

    logging.warning(
        "Port-forward health check did not pass within %.0fs. "
        "Continuing anyway (Run() will fail if agent is unreachable).",
        timeout,
    )


def StopPortForward():
    """Stop the port-forward subprocess and clean up."""
    _port_forward_manager.stop()
    logging.info("Port-forward stopped.")
