#!/usr/bin/env python3
"""Minimal QPS benchmark script (Scheduling Throughput).

Runs inside the Agent Sandbox to validate claim readiness.
Executes a trivial operation and reports status.  The orchestrator-side
timing (orchestrator_total_ms) serves as the primary TTFE measurement —
when the warm pool drains, that metric spikes because fresh pods must be
cold-started.
"""
import json
import time

t0 = time.perf_counter()

# Trivial computation to prove the sandbox is functional
result = sum(range(10_000))

elapsed_ms = (time.perf_counter() - t0) * 1000

status = "success" if result == 49995000 else "fail"
print(json.dumps({
    "sandbox_status": status,
    "sandbox_exec_ms": round(elapsed_ms, 3),
}))
