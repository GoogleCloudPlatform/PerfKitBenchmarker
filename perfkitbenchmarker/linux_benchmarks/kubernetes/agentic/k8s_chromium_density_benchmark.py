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

"""PKB Benchmark: GKE Agent Chromium Density Saturation .

Atomic single-point measurement of Chromium browser sandbox density on a
pre-provisioned GKE cluster with gVisor isolation. Measures interaction
latency, screenshot generation time, cold start, navigation, evaluation,
fill, click latencies, and RSS memory at a given concurrent session count.

This benchmark is designed to be invoked repeatedly by an external sweep
controller that varies the density parameter across iterations to find
the saturation point.

Usage:
  python pkb.py --benchmarks=gke_chromium_density \\
                --k8s_chromium_density_concurrent_sessions=4 \\
                --k8s_chromium_density_task_count=10 \\
                --k8s_chromium_density_warmup_tasks=5 \\
                --k8s_agentic_namespace=agentic \\
                --k8s_agentic_agent_api_url=http://localhost:8080

Samples emitted (per run):
  - gke_chromium_density_interaction_mean      (ms)
  - gke_chromium_density_interaction_p95       (ms)
  - gke_chromium_density_navigate_mean         (ms)
  - gke_chromium_density_navigate_p95          (ms)
  - gke_chromium_density_evaluate_mean         (ms)
  - gke_chromium_density_evaluate_p95          (ms)
  - gke_chromium_density_fill_mean             (ms)
  - gke_chromium_density_fill_p95              (ms)
  - gke_chromium_density_click_mean            (ms)
  - gke_chromium_density_click_p95             (ms)
  - gke_chromium_density_screenshot_mean       (ms)
  - gke_chromium_density_screenshot_p95        (ms)
  - gke_chromium_density_cold_start_mean       (ms)
  - gke_chromium_density_cold_start_p95        (ms)
  - gke_chromium_density_rss_end               (MB)
  - gke_chromium_density_rss_growth            (MB)
  - gke_chromium_density_wall_time             (seconds)
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

BENCHMARK_NAME = "k8s_chromium_density"
BENCHMARK_CONFIG = """
k8s_chromium_density:
  description: >
    Atomic single-point Chromium browser sandbox density measurement on a
    pre-provisioned GKE cluster with gVisor isolation.
  flags: {}
  container_registry: {}
  container_specs: {}
  container_cluster: {}
"""

_WARMPOOL_NAME = "chromium-sandbox-warmpool"
_WARMPOOL_LABEL = "sandbox=chromium-sandbox-example"

# ---------------------------------------------------------------------------
# Benchmark-specific flags
# ---------------------------------------------------------------------------

flags.DEFINE_integer(
    "k8s_chromium_density_concurrent_sessions",
    1,
    "Number of concurrent Chromium browser sessions to run.",
)

flags.DEFINE_integer(
    "k8s_chromium_density_task_count",
    10,
    "Number of browser task iterations per Chromium session.",
)

flags.DEFINE_integer(
    "k8s_chromium_density_warmup_tasks",
    5,
    "Number of warmup iterations per session (excluded from stats).",
)

flags.DEFINE_bool(
    "k8s_chromium_density_patch_warmpool",
    True,
    "Patch SandboxWarmPool replicas to match density before measurement.",
)

flags.DEFINE_integer(
    "k8s_chromium_density_exec_timeout",
    120,
    "Sandbox command execution timeout in seconds.",
)

flags.DEFINE_integer(
    "k8s_chromium_density_provision_timeout",
    300,
    "Max seconds to wait for warm pool pods to reach Running.",
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
    """Execute a single Chromium density measurement and return samples.

    Returns:
      List of sample.Sample objects.
    """
    utils.set_benchmark_spec(benchmark_spec)

    ns = FLAGS.k8s_agentic_namespace
    density = FLAGS.k8s_chromium_density_concurrent_sessions

    logging.info("=== Run: chromium_density=%d ===", density)

    # Ensure port-forward is active (needed when sweeps skip Prepare)
    utils.EnsurePortForward()

    # Patch warm pool (moved from Prepare for sweep compatibility)
    if FLAGS.k8s_chromium_density_patch_warmpool:
        utils.PatchWarmPool(
            namespace=ns,
            warmpool_name=_WARMPOOL_NAME,
            replicas=density,
            label=_WARMPOOL_LABEL,
            wait_timeout=FLAGS.k8s_chromium_density_provision_timeout,
        )

    # POST to agent API
    payload = {
        "task_count": FLAGS.k8s_chromium_density_task_count,
        "warmup_tasks": FLAGS.k8s_chromium_density_warmup_tasks,
        "concurrent_sessions": density,
        "sandbox_exec_timeout_s": FLAGS.k8s_chromium_density_exec_timeout,
    }

    t0 = time.monotonic()
    result = utils.CallAgentApi("/benchmark/chromium/density", payload)
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
        "density": density,
        "successful_sessions": successful,
        "failed_sessions": failed,
        "task_count": FLAGS.k8s_chromium_density_task_count,
        "warmup_tasks": FLAGS.k8s_chromium_density_warmup_tasks,
        "wall_time_s": round(wall_time, 2),
    }

    samples = []

    # Per-task-type latency: mean and P95 for each
    utils.EmitPercentileStats(BENCHMARK_NAME, samples, agg, "interaction", ["mean", "p95"], "ms", ns, extra)
    
    utils.EmitPercentileStats(BENCHMARK_NAME, samples, agg, "navigate", ["mean", "p95"], "ms", ns, extra)
    
    utils.EmitPercentileStats(BENCHMARK_NAME, samples, agg, "evaluate", ["mean", "p95"], "ms", ns, extra)
    
    utils.EmitPercentileStats(BENCHMARK_NAME, samples, agg, "fill", ["mean", "p95"], "ms", ns, extra)
    
    utils.EmitPercentileStats(BENCHMARK_NAME, samples, agg, "click", ["mean", "p95"], "ms", ns, extra)
    
    utils.EmitPercentileStats(BENCHMARK_NAME, samples, agg, "screenshot", ["mean", "p95"], "ms", ns, extra)
    
    utils.EmitPercentileStats(BENCHMARK_NAME, samples, agg, "cold_start", ["mean", "p95"], "ms", ns, extra)

    # RSS memory
    utils.EmitSampleIfPresent(BENCHMARK_NAME, samples, agg, "rss_end_mb", "rss_end", "MB", ns, extra)

    # Session counts (always emitted, even on total failure)
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

    # Wall time
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_wall_time",
            round(wall_time, 2),
            "seconds",
            ns,
            extra,
        )
    )

    logging.info("Emitted %d samples for chromium_density=%d.", len(samples), density)
    psi_data = utils.ScrapePsi(ns)
    for k, v in psi_data.items():
        samples.append(utils.MakeSample(f"{BENCHMARK_NAME}_{k}", v, "percent", ns, extra))

    return samples

def Cleanup(benchmark_spec: object) -> None:
    """Clean up after measurement. Delete claims and drain warm pool."""
    ns = FLAGS.k8s_agentic_namespace
    logging.info("Cleanup: deleting SandboxClaims and draining warm pool.")

    # Delete any lingering SandboxClaims to release claimed pods
    utils.RunKubectl(
        [
            "delete",
            "sandboxclaims",
            "-l",
            _WARMPOOL_LABEL,
            "-n",
            ns,
            "--ignore-not-found=true",
        ],
        timeout=60,
        raise_on_failure=False,
    )

    # Drain warm pool to 0
    utils.DrainWarmPool(
        namespace=ns,
        warmpool_name=_WARMPOOL_NAME,
        label=_WARMPOOL_LABEL,
    )

    utils.StopPortForward()
    logging.info("Cleanup complete (cluster persists).")

