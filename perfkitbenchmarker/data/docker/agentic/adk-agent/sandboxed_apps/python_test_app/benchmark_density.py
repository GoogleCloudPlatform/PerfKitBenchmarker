#!/usr/bin/env python3
"""
Agentic Python Sandbox Benchmark
Measures: TTFE (Time to First Execution), CEL (Command Execution Latency), RSS Memory

Three task categories:
  - compute: CPU-bound (matrix multiply, sorting large lists)
  - syscall:  gVisor Sentry stress (large file I/O, many stat calls)
  - import:   Gofer FS I/O + memory (import heavy stdlib, build data)

Metrics: all sandbox_* keys.
"""
import time
import json
import os
import resource
import sys
import math
import random
import warnings

warnings.filterwarnings("ignore")

SAMPLE_COUNT = int(os.environ.get("SAMPLE_COUNT") or "20")
SAMPLE_WARMUP = int(os.environ.get("SAMPLE_WARMUP") or "0")

print(f"SAMPLE_COUNT: {SAMPLE_COUNT}")
print(f"SAMPLE_WARMUP: {SAMPLE_WARMUP}")

# ── Persistent allocations (retained across iterations to grow RSS) ──
# ~20MB baseline allocation that stays resident
_RESIDENT_DATA = [bytearray(1024 * 1024) for _ in range(20)]  # 20 × 1MB


def get_rss_mb():
    """Get current RSS memory in MB."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def get_static_tasks():
    """Return deterministic static tasks to measure execution latency.

    Three task categories enable decomposition of CEL degradation:
      - compute: sort a 100k-element list + matrix-like multiply
      - syscall:  write/read 1MB temp files, 2000 stat calls
      - import:   import 15 heavy stdlib modules + build large dicts
    """
    return [
        {
            "id": 1,
            "type": "compute",
            "code": (
                "import math, random\n"
                "random.seed(42)\n"
                "data = [random.random() for _ in range(100_000)]\n"
                "data.sort()\n"
                "# Matrix-like multiply (flattened 200×200)\n"
                "a = list(range(40_000))\n"
                "b = [x * 0.001 for x in a]\n"
                "_ = sum(x * y for x, y in zip(a, b))\n"
            ),
        },
        {
            "id": 2,
            "type": "syscall",
            "code": (
                "import os, tempfile\n"
                "d = tempfile.gettempdir()\n"
                "# Write + read 1MB file through gVisor Gofer\n"
                "path = os.path.join(d, 'bench_heavy.bin')\n"
                "data = b'x' * (1024 * 1024)\n"
                "with open(path, 'wb') as f:\n"
                "    f.write(data)\n"
                "with open(path, 'rb') as f:\n"
                "    _ = f.read()\n"
                "os.unlink(path)\n"
                "# Heavy stat/listdir\n"
                "[os.stat(d) for _ in range(1000)]\n"
                "[os.listdir(d) for _ in range(1000)]\n"
            ),
        },
        {
            "id": 3,
            "type": "import",
            "code": (
                "import importlib, sys\n"
                "mods = [\n"
                "    'json', 'csv', 'html', 'email', 'unittest', 'logging',\n"
                "    'xml.etree.ElementTree', 'http.client', 'urllib.request',\n"
                "    'argparse', 'pprint', 'textwrap', 'difflib',\n"
                "]\n"
                "for _ in range(20):\n"
                "    for m in mods:\n"
                "        try:\n"
                "            sys.modules.pop(m, None)\n"
                "            importlib.import_module(m)\n"
                "        except Exception:\n"
                "            pass\n"
                "# Build a large dict to add memory pressure\n"
                "_ = {str(i): list(range(100)) for i in range(10_000)}\n"
            ),
        },
    ]


def _percentile(sorted_vals, pct):
    """Return the value at the given percentile from a pre-sorted list."""
    idx = int(len(sorted_vals) * pct)
    return sorted_vals[min(idx, len(sorted_vals) - 1)]


def run_benchmark():
    results = {"ttfe_ms": None, "cel_ms": [], "rss_mb_start": None, "rss_mb_end": None}

    # Measure TTFE
    ttfe_start = time.perf_counter()
    exec("x = 1 + 1", globals())
    results["ttfe_ms"] = round((time.perf_counter() - ttfe_start) * 1000, 6)

    results["rss_mb_start"] = get_rss_mb()

    tasks = get_static_tasks()
    sampled_tasks = [t for t in tasks if t["type"] != "import"]
    import_task = next((t for t in tasks if t["type"] == "import"), None)

    # Warmup — sampled tasks only (import uses C-extension modules that
    # error on repeated reimport, so it runs once outside the loop)
    for _ in range(SAMPLE_WARMUP):
        for task in sampled_tasks:
            exec(task["code"], globals())

    # Benchmark iterations — compute + syscall only
    for i in range(SAMPLE_COUNT):
        # Grow resident memory slightly each iteration (~100KB)
        _RESIDENT_DATA.append(bytearray(100 * 1024))

        for task in sampled_tasks:
            start = time.perf_counter()
            exec(task["code"], globals())
            elapsed_ms = round((time.perf_counter() - start) * 1000, 6)
            results["cel_ms"].append({
                "iteration": i,
                "task_id": task["id"],
                "type": task["type"],
                "latency_ms": elapsed_ms,
            })

    # Import task — single run (C-extension modules break on repeated reimport)
    import_elapsed_ms = 0.0
    if import_task:
        import_start = time.perf_counter()
        exec(import_task["code"], globals())
        import_elapsed_ms = round((time.perf_counter() - import_start) * 1000, 6)

    results["rss_mb_end"] = get_rss_mb()

    # --- Raw per-iteration totals (compute + syscall) ---
    iteration_totals = []
    for i in range(SAMPLE_COUNT):
        total = sum(r["latency_ms"] for r in results["cel_ms"] if r["iteration"] == i)
        iteration_totals.append(round(total, 6))

    # --- Raw per-type latencies ---
    types_seen = sorted(set(r["type"] for r in results["cel_ms"]))
    per_type_raw = {}
    for t in types_seen:
        per_type_raw[t] = [round(r["latency_ms"], 6)
                           for r in results["cel_ms"] if r["type"] == t]

    # Output raw arrays — cross-sandbox stats computed by main.py
    summary = {
        "hostname": os.environ.get("HOSTNAME", "unknown"),
        "sandbox_ttfe_ms": results["ttfe_ms"],
        "sandbox_total_cel_ms": iteration_totals,
        "sandbox_import_cel_ms": import_elapsed_ms,
        "sandbox_rss_start_mb": results["rss_mb_start"],
        "sandbox_rss_end_mb": results["rss_mb_end"],
        "sandbox_rss_growth_mb": round(results["rss_mb_end"] - results["rss_mb_start"], 6),
        "sample_count": SAMPLE_COUNT,
        "sample_warmup": SAMPLE_WARMUP,
        "total_iterations": len(iteration_totals),
        "task_types": len(types_seen) + (1 if import_task else 0),
    }

    for t, raw in per_type_raw.items():
        summary[f"sandbox_{t}_cel_ms"] = raw

    print(json.dumps(summary))

    with open("/tmp/benchmark_results.json", "w") as f:
        json.dump(results, f)

    return summary

if __name__ == "__main__":
    run_benchmark()
