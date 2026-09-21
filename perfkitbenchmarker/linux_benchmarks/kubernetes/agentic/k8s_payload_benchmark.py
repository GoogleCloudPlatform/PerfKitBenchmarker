# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""PKB Benchmark: GKE Agent Payload Transfer Saturation .

Atomic single-point measurement of payload transfer latency from a gVisor
sandbox back to the orchestrator on a pre-provisioned GKE cluster.  Measures
generation time, serialization time, stdout write time, total transfer time,
throughput, and RSS at a given payload_size_mb and concurrent_sessions count.

This benchmark is designed to be invoked repeatedly by an external sweep
controller that varies the payload_size_mb parameter across iterations to
find the saturation point.

Usage:
  python pkb.py --benchmarks=gke_payload \
                --k8s_payload_size_mb=50 \
                --k8s_payload_iterations=20 \
                --k8s_payload_concurrent_sessions=5 \
                --k8s_agentic_namespace=agentic \
                --k8s_agentic_agent_api_url=http://localhost:8080

Samples emitted (per run):
  - gke_payload_orchestrator_transfer_mean       (ms)
  - gke_payload_orchestrator_transfer_p50        (ms)
  - gke_payload_orchestrator_transfer_p95        (ms)
  - gke_payload_orchestrator_transfer_p99        (ms)
  - gke_payload_orchestrator_transfer_min        (ms)
  - gke_payload_orchestrator_transfer_max        (ms)
  - gke_payload_sandbox_payload_size_bytes       (bytes)
  - gke_payload_sandbox_payload_encoded_size_bytes (bytes)
  - gke_payload_sandbox_payload_iterations       (count)
  - gke_payload_sandbox_generation_time_mean     (ms)
  - gke_payload_sandbox_generation_time_p50      (ms)
  - gke_payload_sandbox_generation_time_p95      (ms)
  - gke_payload_sandbox_generation_time_p99      (ms)
  - gke_payload_sandbox_generation_time_min      (ms)
  - gke_payload_sandbox_generation_time_max      (ms)
  - gke_payload_sandbox_serialization_time_mean  (ms)
  - gke_payload_sandbox_serialization_time_p50   (ms)
  - gke_payload_sandbox_serialization_time_p95   (ms)
  - gke_payload_sandbox_serialization_time_p99   (ms)
  - gke_payload_sandbox_serialization_time_min   (ms)
  - gke_payload_sandbox_serialization_time_max   (ms)
  - gke_payload_sandbox_stdout_time_mean         (ms)
  - gke_payload_sandbox_stdout_time_p50          (ms)
  - gke_payload_sandbox_stdout_time_p95          (ms)
  - gke_payload_sandbox_stdout_time_p99          (ms)
  - gke_payload_sandbox_stdout_time_min          (ms)
  - gke_payload_sandbox_stdout_time_max          (ms)
  - gke_payload_sandbox_transfer_time_mean       (ms)
  - gke_payload_sandbox_transfer_time_p50        (ms)
  - gke_payload_sandbox_transfer_time_p95        (ms)
  - gke_payload_sandbox_transfer_time_p99        (ms)
  - gke_payload_sandbox_transfer_time_min        (ms)
  - gke_payload_sandbox_transfer_time_max        (ms)
  - gke_payload_sandbox_throughput_mean           (MB/s)
  - gke_payload_sandbox_throughput_p50            (MB/s)
  - gke_payload_sandbox_throughput_min            (MB/s)
  - gke_payload_sandbox_rss_start                (MB)
  - gke_payload_sandbox_rss_end                  (MB)
  - gke_payload_sandbox_rss_growth               (MB)
  - gke_payload_wall_time                        (seconds)
"""

from __future__ import annotations

import logging
import time
import uuid

from absl import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import configs
from perfkitbenchmarker.linux_benchmarks.kubernetes.agentic import (
    k8s_benchmark_utils as utils,
)
from perfkitbenchmarker.linux_benchmarks.kubernetes.agentic import (
    gke_deploy_utils as deploy_utils,
)

FLAGS = flags.FLAGS

BENCHMARK_NAME = "k8s_payload"
BENCHMARK_CONFIG = """
k8s_payload:
  description: >
    Atomic single-point payload transfer saturation measurement on a
    pre-provisioned GKE cluster with gVisor isolation.
  flags: {}
  container_registry: {}
  container_specs: {}
  container_cluster: {}
"""

_WARMPOOL_NAME = "python-sandbox-warmpool"
_WARMPOOL_LABEL = "sandbox=python-sandbox-example"

# ---------------------------------------------------------------------------
# Benchmark-specific flags
# ---------------------------------------------------------------------------

flags.DEFINE_float(
    "k8s_payload_size_mb",
    1.0,
    "Payload size in megabytes to transfer from the sandbox.",
)

flags.DEFINE_integer(
    "k8s_payload_iterations",
    20,
    "Number of transfer iterations per sandbox session.",
)

flags.DEFINE_integer(
    "k8s_payload_concurrent_sessions",
    5,
    "Number of parallel sandbox sessions.",
)

flags.DEFINE_integer(
    "k8s_payload_exec_timeout",
    300,
    "Sandbox command execution timeout in seconds.",
)

flags.DEFINE_bool(
    "k8s_payload_patch_warmpool",
    True,
    "Patch SandboxWarmPool replicas to match concurrent_sessions before measurement.",
)

# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def GetConfig(user_config: dict) -> dict:
    """Load and return benchmark config.

    No vm_groups — PKB skips Provision() and Teardown().
    """
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

def Prepare(benchmark_spec: object) -> None:
    """Deploy workloads and verify agent API."""
    benchmark_spec.always_call_cleanup = True
    logging.info("=== Prepare: deploying workloads ===")
    deploy_utils.DeployWorkloads(benchmark_spec)
    utils.CheckAgentHealthz(required=False)
    utils.EnsurePortForward()
    logging.info("Prepare complete.")

def Run(benchmark_spec: object) -> list[sample.Sample]:
    """Execute a single payload transfer measurement and return samples.

    Returns:
      List of sample.Sample objects.
    """
    utils.set_benchmark_spec(benchmark_spec)

    ns = FLAGS.k8s_agentic_namespace
    payload_size_mb = FLAGS.k8s_payload_size_mb
    iterations = FLAGS.k8s_payload_iterations
    concurrent = FLAGS.k8s_payload_concurrent_sessions

    logging.info(
        "=== Run: payload_size_mb=%s, iterations=%d, concurrent=%d ===",
        payload_size_mb,
        iterations,
        concurrent,
    )

    # Ensure port-forward is active (needed when sweeps skip Prepare)
    utils.EnsurePortForward()

    # Patch warm pool (moved from Prepare for sweep compatibility)
    if FLAGS.k8s_payload_patch_warmpool:
        utils.PatchWarmPool(
            namespace=ns,
            warmpool_name=_WARMPOOL_NAME,
            replicas=concurrent,
            label=_WARMPOOL_LABEL,
            wait_timeout=600
        )

    # POST to agent API
    payload = {
        "payload_size_mb": payload_size_mb,
        "payload_iterations": iterations,
        "concurrent_sessions": concurrent,
        "sandbox_exec_timeout_s": FLAGS.k8s_payload_exec_timeout,
    }

    t0 = time.monotonic()
    result = utils.CallAgentApi("/benchmark/python/payload", payload)
    wall_time = time.monotonic() - t0

    successful = result.get("successful_sessions", 0)
    failed = result.get("failed_sessions", 0)
    agg = result.get("aggregate") or {}

    logging.info(
        "API response: %d successful, %d failed sessions (%.1fs)",
        successful,
        failed,
        wall_time,
    )

    # Build samples
    run_id = str(uuid.uuid4())[:8]

    # Dictionary of extra metadata key-value pairs appended to every sample.
    # Used for downstream dashboard filtering and correlating runs.
    extra = {
        "run_id": run_id,
        "payload_size_mb": payload_size_mb,
        "payload_iterations": iterations,
        "concurrent_sessions": concurrent,
        "wall_time_s": round(wall_time, 2),
    }

    samples = []

    # Orchestrator-side transfer latency
    utils.EmitPercentileStats(BENCHMARK_NAME, samples, agg, "orchestrator_transfer", ["mean", "p50", "p95", "p99", "min", "max"], "ms", ns, extra)

    


    # Payload metadata
    utils.EmitSampleIfPresent(
        BENCHMARK_NAME,
        samples,
        agg,
        "sandbox_payload_size_bytes",
        "sandbox_payload_size_bytes",
        "bytes",
        ns,
        extra,
    )
    utils.EmitSampleIfPresent(
        BENCHMARK_NAME,
        samples,
        agg,
        "sandbox_payload_encoded_size_bytes",
        "sandbox_payload_encoded_size_bytes",
        "bytes",
        ns,
        extra,
    )
    utils.EmitSampleIfPresent(
        BENCHMARK_NAME,
        samples,
        agg,
        "sandbox_payload_iterations",
        "sandbox_payload_iterations",
        "count",
        ns,
        extra,
    )

    # Generation time (os.urandom)
    utils.EmitPercentileStats(BENCHMARK_NAME, samples, agg, "sandbox_generation_time", ["mean", "p50", "p95", "p99", "min", "max"], "ms", ns, extra)

    


    # Serialization time (base64 encode)
    utils.EmitPercentileStats(BENCHMARK_NAME, samples, agg, "sandbox_serialization_time", ["mean", "p50", "p95", "p99", "min", "max"], "ms", ns, extra)

    


    # Stdout write time (gVisor Gofer write syscall)
    utils.EmitPercentileStats(BENCHMARK_NAME, samples, agg, "sandbox_stdout_time", ["mean", "p50", "p95", "p99", "min", "max"], "ms", ns, extra)

    


    # Transfer time (serialization + stdout write — threshold metric)
    utils.EmitPercentileStats(BENCHMARK_NAME, samples, agg, "sandbox_transfer_time", ["mean", "p50", "p95", "p99", "min", "max"], "ms", ns, extra)

    


    # Throughput
    utils.EmitPercentileStats(BENCHMARK_NAME, samples, agg, "sandbox_throughput", ["mean", "p50", "min"], "MB/s", ns, extra)


    # RSS
    utils.EmitSampleIfPresent(BENCHMARK_NAME, samples, agg, "sandbox_rss_start_mb", "sandbox_rss_start", "MB", ns, extra)
    utils.EmitSampleIfPresent(BENCHMARK_NAME, samples, agg, "sandbox_rss_end_mb", "sandbox_rss_end", "MB", ns, extra)
    utils.EmitSampleIfPresent(BENCHMARK_NAME, samples, agg, "sandbox_rss_growth_mb", "sandbox_rss_growth", "MB", ns, extra)

    # Wall time
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_successful_sessions",
            float(successful),
            "count",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_failed_sessions",
            float(failed),
            "count",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_wall_time",
            round(wall_time, 2),
            "seconds",
            ns,
            extra,
        )
    )

    logging.info(
        "Emitted %d samples for payload_size_mb=%s.", len(samples), payload_size_mb
    )
    psi_data = utils.ScrapePsi(ns)
    for k, v in psi_data.items():
        samples.append(utils.MakeSample(f"{BENCHMARK_NAME}_{k}", v, "percent", ns, extra))

    return samples

def Cleanup(benchmark_spec: object) -> None:
    """Clean up after measurement. Scale warm pool to 0."""
    ns = FLAGS.k8s_agentic_namespace
    logging.info("Cleanup: draining warm pool.")

    utils.DrainWarmPool(
        namespace=ns,
        warmpool_name=_WARMPOOL_NAME,
        label=_WARMPOOL_LABEL,
    )

    utils.StopPortForward()
    logging.info("Cleanup complete (cluster persists).")
