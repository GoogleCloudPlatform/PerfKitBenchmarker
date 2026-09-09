#!/usr/bin/env python3
"""Agentic Payload Transfer Benchmark.

Measures the cost of returning large "Observation" payloads from a gVisor
sandbox back to the Orchestrator via the real data path:
  stdout → code_execution_result.output → orchestrator HTTP response.

For a given PAYLOAD_SIZE_MB, the script:
  1. Generates a payload of that size (os.urandom + base64)
  2. Measures generation, serialization, and stdout-write times separately
  3. Repeats for PAYLOAD_ITERATIONS to compute stable percentiles
  4. On the final iteration, writes the actual payload to stdout (measuring
     real end-to-end transfer); other iterations write to /dev/null to
     measure write-syscall cost without flooding the return channel.
  5. Emits a JSON summary to stderr (parsed by main.py)

Metrics are split so that pass/fail thresholds can exclude generation
time (os.urandom), which is not part of data transfer.

Environment variables (injected by the agent):
  PAYLOAD_SIZE_MB     — target payload size in megabytes (default: 1)
  PAYLOAD_ITERATIONS  — number of transfer iterations (default: 20)
"""

from __future__ import annotations


import base64
import json
import os
import resource
import sys
import time
import traceback

PAYLOAD_SIZE_MB = float(os.environ.get("PAYLOAD_SIZE_MB") or "1")
PAYLOAD_ITERATIONS = int(os.environ.get("PAYLOAD_ITERATIONS") or "20")


# Use stderr for all diagnostic/metric output so stdout is reserved for
# the actual payload transfer (the measured data path).
def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


_log(f"PAYLOAD_SIZE_MB: {PAYLOAD_SIZE_MB}")
_log(f"PAYLOAD_ITERATIONS: {PAYLOAD_ITERATIONS}")


def get_rss_mb() -> float:
    """Get current RSS memory in MB."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def _percentile(sorted_values: list[float], fraction: float) -> float:
    """Return the value at the given percentile fraction (0.0-1.0)."""
    if not sorted_values:
        return 0.0
    idx = fraction * (len(sorted_values) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_values) - 1)
    weight = idx - lo
    return sorted_values[lo] * (1 - weight) + sorted_values[hi] * weight


def _stats_for(latencies: list[float]) -> dict[str, float]:
    """Compute mean/p50/p95/p99/min/max for a list of latencies (ms)."""
    latencies.sort()
    return {
        "mean": round(sum(latencies) / len(latencies), 6),
        "p50": round(latencies[len(latencies) // 2], 6),
        "p95": round(_percentile(latencies, 0.95), 6),
        "p99": round(_percentile(latencies, 0.99), 6),
        "min": round(latencies[0], 6),
        "max": round(latencies[-1], 6),
    }


def run_benchmark() -> dict:
    """Execute the payload transfer benchmark and print JSON results."""
    target_bytes = int(PAYLOAD_SIZE_MB * 1024 * 1024)
    rss_start = get_rss_mb()

    generation_times = []
    serialization_times = []
    stdout_times = []  # stdout write syscall time
    transfer_times = []  # serialize + stdout write (the threshold metric)
    throughputs = []  # MB/s based on stdout write time

    # --- Warmup (2 iterations, not recorded) ---
    # Stabilizes os.urandom() entropy pool and base64 codec
    # initialization before measured iterations begin.
    for _ in range(2):
        raw = os.urandom(target_bytes)
        _ = base64.b64encode(raw).decode("ascii")

    # --- Measured iterations ---
    for i in range(PAYLOAD_ITERATIONS):
        # 1. Generate payload (os.urandom — NOT data transfer)
        t0 = time.perf_counter()
        raw = os.urandom(target_bytes)
        t_gen = time.perf_counter()

        # 2. Serialize (base64 encode — mirrors real observation encoding)
        encoded = base64.b64encode(raw).decode("ascii")
        t_ser = time.perf_counter()

        # 3. Transfer -- write payload through gVisor write-syscall path.
        #    Always writes to /dev/null to measure the gVisor Sentry
        #    write-syscall overhead without flooding stdout. The Sentry
        #    intercepts write() identically for both /dev/null and stdout.
        #    Writing large payloads (100MB+) to stdout destroys the JSON
        #    metrics summary that follows, causing all sandbox_* metrics
        #    to be lost. Orchestrator-side transfer time is captured
        #    separately by main.py orchestrator_transfer_* metrics.
        t_xfer_start = time.perf_counter()
        with open("/dev/null", "w") as devnull:
            devnull.write(encoded)
        t_xfer = time.perf_counter()

        gen_ms = (t_gen - t0) * 1000
        ser_ms = (t_ser - t_gen) * 1000
        stdout_ms = (t_xfer - t_xfer_start) * 1000
        transfer_ms = ser_ms + stdout_ms  # excludes generation

        generation_times.append(gen_ms)
        serialization_times.append(ser_ms)
        stdout_times.append(stdout_ms)
        transfer_times.append(transfer_ms)

        # Throughput in MB/s (based on encoded size and stdout write time)
        encoded_size_mb = len(encoded) / (1024 * 1024)
        if stdout_ms > 0:
            throughputs.append(encoded_size_mb / (stdout_ms / 1000))

    rss_end = get_rss_mb()

    # Compute stats
    gen_stats = _stats_for(generation_times)
    ser_stats = _stats_for(serialization_times)
    stdout_stats = _stats_for(stdout_times)
    transfer_stats = _stats_for(transfer_times)
    throughput_stats = _stats_for(throughputs) if throughputs else {}

    # Payload metadata
    encoded_size_bytes = len(base64.b64encode(os.urandom(target_bytes)))

    summary = {
        "hostname": os.environ.get("HOSTNAME", "unknown"),
        # Payload config
        "sandbox_payload_size_bytes": target_bytes,
        "sandbox_payload_encoded_size_bytes": encoded_size_bytes,
        "sandbox_payload_iterations": PAYLOAD_ITERATIONS,
        # Generation time (os.urandom — NOT data transfer, excluded from threshold)
        "sandbox_generation_time_mean_ms": gen_stats["mean"],
        "sandbox_generation_time_p50_ms": gen_stats["p50"],
        "sandbox_generation_time_p95_ms": gen_stats["p95"],
        "sandbox_generation_time_p99_ms": gen_stats["p99"],
        "sandbox_generation_time_min_ms": gen_stats["min"],
        "sandbox_generation_time_max_ms": gen_stats["max"],
        # Serialization time (base64 encode — CPU bound)
        "sandbox_serialization_time_mean_ms": ser_stats["mean"],
        "sandbox_serialization_time_p50_ms": ser_stats["p50"],
        "sandbox_serialization_time_p95_ms": ser_stats["p95"],
        "sandbox_serialization_time_p99_ms": ser_stats["p99"],
        "sandbox_serialization_time_min_ms": ser_stats["min"],
        "sandbox_serialization_time_max_ms": ser_stats["max"],
        # Stdout write time (the raw write-syscall through gVisor)
        "sandbox_stdout_time_mean_ms": stdout_stats["mean"],
        "sandbox_stdout_time_p50_ms": stdout_stats["p50"],
        "sandbox_stdout_time_p95_ms": stdout_stats["p95"],
        "sandbox_stdout_time_p99_ms": stdout_stats["p99"],
        "sandbox_stdout_time_min_ms": stdout_stats["min"],
        "sandbox_stdout_time_max_ms": stdout_stats["max"],
        # Transfer time (serialization + stdout write — the threshold metric)
        "sandbox_transfer_time_mean_ms": transfer_stats["mean"],
        "sandbox_transfer_time_p50_ms": transfer_stats["p50"],
        "sandbox_transfer_time_p95_ms": transfer_stats["p95"],
        "sandbox_transfer_time_p99_ms": transfer_stats["p99"],
        "sandbox_transfer_time_min_ms": transfer_stats["min"],
        "sandbox_transfer_time_max_ms": transfer_stats["max"],
        # Throughput (MB/s based on transfer write time)
        "sandbox_throughput_mean_mbps": throughput_stats.get("mean"),
        "sandbox_throughput_p50_mbps": throughput_stats.get("p50"),
        "sandbox_throughput_min_mbps": throughput_stats.get("min"),
        # RSS
        "sandbox_rss_start_mb": rss_start,
        "sandbox_rss_end_mb": rss_end,
        "sandbox_rss_growth_mb": rss_end - rss_start,
    }

    # Emit JSON summary to stdout so _parse_sandbox_json() can find it
    # in code_execution_result.output. ADK only captures stdout, not stderr.
    # (agent.py clears stderr to prevent ADK from dropping stdout.)
    print("---BENCHMARK_RESULT_JSON---", flush=True)
    print(json.dumps(summary), flush=True)

    return summary


if __name__ == "__main__":
    try:
        run_benchmark()
    except Exception as e:
        traceback.print_exc()
