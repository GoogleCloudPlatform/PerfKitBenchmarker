#!/usr/bin/env python3
"""Optimization Sweep Runner - Multi-Benchmark.

Runs baseline + variant benchmarks for any GKE agentic benchmark.
Supports both Static sweeps (comma-separated lists) and Binary Search sweeps
to automatically find the saturation point based on a target metric threshold.

Usage (Static):
  python sweep.py --project my-project --benchmark k8s_python_density --variant baseline --sweep-values 20,40,60,80

Usage (Binary Search):
  python sweep.py --project my-project --benchmark k8s_python_density --variant baseline \
      --search-mode binary --search-min 10 --search-max 200 --search-convergence 5 \
      --threshold-metric k8s_python_density_sandbox_total_cel_mean_ms --threshold-value 2000.0
"""

import argparse
import json
import re
import ipaddress
import logging
import os
import statistics
import subprocess
import sys
import time
import urllib.request
import yaml
from jinja2 import Template as Jinja2Template

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ============================================================
# Path Resolution
# ============================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _find_repo_root():
    d = SCRIPT_DIR
    for _ in range(10):
        if os.path.isfile(os.path.join(d, "pkb.py")) and os.path.isdir(
            os.path.join(d, "perfkitbenchmarker")
        ):
            return d
        parent = os.path.dirname(d)
        if parent == d:
            break
        d = parent
    return None


REPO_ROOT = _find_repo_root()
if not REPO_ROOT:
    logger.error(
        "Cannot find pkb.py. Ensure this script is inside the PerfKitBenchmarker repo."
    )
    sys.exit(1)

CONFIG_DIR = os.path.join(
    REPO_ROOT, "perfkitbenchmarker", "data", "k8s_agents", "config", "variants"
)
RESULTS_BASE = os.path.join(REPO_ROOT, "results", "pkb")
TUNING_DIR = os.path.join(CONFIG_DIR, "tuning")
PKB_CMD = "pkb.py"

# ============================================================
# Benchmark Registry
# ============================================================
BENCHMARKS = {
    "k8s_python_density": {
        "sweep_flag": "--k8s_python_density_concurrent_sandbox_count",
        "sweep_label": "density",
        "warmpool": "python-sandbox-warmpool",
        "pod_label": "sandbox=python-sandbox-example",
        "default_sweep": [1, 4, 8, 16, 32],
        "drain_between": True,
    },
    "k8s_chromium_density": {
        "sweep_flag": "--k8s_chromium_density_concurrent_sessions",
        "sweep_label": "density",
        "warmpool": "chromium-sandbox-warmpool",
        "pod_label": "sandbox=chromium-sandbox-example",
        "default_sweep": [1, 2, 4, 8],
        "drain_between": True,
    },
    "k8s_payload": {
        "sweep_flag": "--k8s_payload_size_mb",
        "sweep_label": "payload_size_mb",
        "warmpool": "python-sandbox-warmpool",
        "pod_label": "sandbox=python-sandbox-example",
        "default_sweep": [0.01, 0.1, 1, 5, 10],
        "drain_between": True,
    },
    "k8s_qps": {
        "sweep_flag": "--k8s_qps_target_qps",
        "sweep_label": "target_qps",
        "warmpool": "python-sandbox-warmpool",
        "pod_label": "sandbox=python-sandbox-example",
        "default_sweep": [1, 5, 10, 20, 50],
        "drain_between": True,
    },
    "k8s_warmpool": {
        "sweep_flag": "--k8s_warmpool_target_replicas",
        "sweep_label": "target_replicas",
        "warmpool": "python-sandbox-warmpool",
        "pod_label": "sandbox=python-sandbox-example",
        "default_sweep": [10, 50, 100, 200, 500],
        "drain_between": True,
    },
    "k8s_deletion": {
        "sweep_flag": "--k8s_deletion_batch_size",
        "sweep_label": "batch_size",
        "warmpool": "python-sandbox-warmpool",
        "pod_label": "sandbox=python-sandbox-example",
        "default_sweep": [10, 50, 100, 200, 500],
        "drain_between": True,
    },
    "k8s_snapshot": {
        "sweep_flag": "--k8s_snapshot_preload_mb",
        "sweep_label": "preload_mb",
        "warmpool": None,
        "pod_label": "app=snapshot-benchmark-workload",
        "default_sweep": [10, 50, 100, 500, 1000],
        "drain_between": False,
    },
}

# ============================================================
# Variant Registry
# ============================================================
ALL_VARIANTS = [
    "baseline",
    "kubelet_pulls",
    "overlay_none",
    "larger_vm",
    "hyperdisk_200gb",
    "thp",
    "sched_tuning",
    "swap",
    "c4d_amd",
    "c4a_arm",
    "multi_node",
    "combined",
]

VARIANT_CONFIGS = {v: f"{v}.yaml" for v in ALL_VARIANTS}

VARIANT_DESC = {
    "baseline": "Baseline: c4-standard-8, gVisor, defaults",
    "kubelet_pulls": "Kubelet parallel pulls + image GC",
    "overlay_none": "gVisor overlay2=none",
    "larger_vm": "c4-standard-16 (2x CPU)",
    "hyperdisk_200gb": "hyperdisk-balanced 200GB",
    "thp": "Transparent Hugepages (THP=always)",
    "sched_tuning": "Kernel scheduler tuning",
    "swap": "Swap enabled (swappiness=15)",
    "c4d_amd": "C4D AMD (c4d-standard-8)",
    "c4a_arm": "C4A ARM (c4a-standard-8)",
    "multi_node": "Multi-node sandbox pool (2x c4-standard-8)",
    "combined": "Combined best knobs",
}


# ============================================================
# Helpers
# ============================================================
def get_my_ip():
    # GKE master-authorized-networks accepts IPv4 CIDRs only, so an IPv6 answer
    # (what ifconfig.me returns on a dual-stack network) must be rejected rather
    # than passed through as "<addr>/32".
    endpoints = (
        "https://api.ipify.org",
        "https://ipv4.icanhazip.com",
        "https://ifconfig.me",
    )
    for url in endpoints:
        try:
            ip = urllib.request.urlopen(url, timeout=10).read().decode().strip()
            ipaddress.IPv4Address(ip)
            return ip
        except Exception:
            continue
    raise RuntimeError(
        "Could not determine a public IPv4 address from any of: "
        + ", ".join(endpoints)
        + ". An IPv4 address is required for --master-authorized-networks."
    )


def run_cmd(cmd, check=True):
    logger.info("CMD: %s", cmd)
    result = subprocess.run(cmd, shell=True)
    if check and result.returncode != 0:
        logger.error("Command failed (rc=%d)", result.returncode)
        sys.exit(1)
    return result.returncode


def drain_warmpool(warmpool_name, pod_label, namespace):
    if not warmpool_name:
        return
    logger.info("Draining warm pool %s to 0", warmpool_name)
    run_cmd(
        f'kubectl patch sandboxwarmpool {warmpool_name} -n {namespace} --type=merge -p \'{{"spec":{{"replicas":0}}}}\'',
        check=False,
    )
    run_cmd(
        f"kubectl delete sandboxclaims --all -n {namespace} --ignore-not-found=true",
        check=False,
    )
    logger.info("Waiting for pods to terminate...")
    run_cmd(
        f"kubectl wait --for=delete pod -l {pod_label} -n {namespace} --timeout=300s",
        check=False,
    )
    logger.info("Warm pool drained")


def _read_yaml_flags(config_path, benchmark, flag_name, flag_type="list"):
    empty = [] if flag_type == "list" else None
    try:
        with open(config_path, "r") as f:
            data = yaml.safe_load(f)
        if not data:
            return empty
        bench_config = data.get(benchmark)
        if bench_config is None:
            for _, bench_config in data.items():
                if isinstance(bench_config, dict):
                    break
            else:
                return empty
        if not isinstance(bench_config, dict):
            return empty
        fl = bench_config.get("flags", {})
        if not isinstance(fl, dict):
            return empty
        value = fl.get(flag_name)
        if value is None:
            return empty
        if flag_type == "list":
            if isinstance(value, list):
                return [str(v).strip() for v in value if v]
            return [str(value).strip()]
        else:
            return str(value).strip()
    except Exception as e:
        logger.warning("Could not read %s from %s: %s", flag_name, config_path, e)
        return empty


def get_variant_uri(benchmark, variant):
    bench_short = benchmark.replace("k8s_", "")[:4]
    clean_var = re.sub(r"[^a-zA-Z0-9]", "", variant)[:4]
    owner = re.sub(r"[^a-z0-9]", "", os.environ.get("USER", "default").lower())[:4]
    return f"{bench_short}{clean_var}{owner}"


def run_pkb(
    benchmark,
    variant,
    stage,
    args,
    extra_flags=None,
    passthrough_flags=None,
    check=True,
):
    config = os.path.join(CONFIG_DIR, VARIANT_CONFIGS[variant])
    uri = get_variant_uri(benchmark, variant)
    results = os.path.join(RESULTS_BASE, benchmark)
    os.makedirs(results, exist_ok=True)

    parts = [
        "python " + PKB_CMD,
        "--benchmarks=" + benchmark,
        "--run_stage=" + stage,
        "--project=" + args.project,
        "--benchmark_config_file=" + config,
        "--run_uri=" + uri,
        "--json_write_mode=a",
        "--temp_dir=" + results,
    ]

    if "provision" in stage:
        parts.append("--owner=" + args.owner)
        if args.network:
            parts.append("--gce_network_name=" + args.network)
        my_ip = get_my_ip()
        gke_flag_list = [
            "--enable-master-authorized-networks",
            "--master-authorized-networks=" + my_ip + "/32",
        ]
        if args.subnet:
            gke_flag_list.append("--subnetwork=" + args.subnet)

        yaml_gke_flags = _read_yaml_flags(config, benchmark, "gke_additional_flags")
        for yf in yaml_gke_flags:
            if yf not in gke_flag_list:
                gke_flag_list.append(yf)
        parts.append("--gke_additional_flags=" + ",".join(gke_flag_list))
        yaml_nodepool_flags = _read_yaml_flags(
            config, benchmark, "gke_additional_nodepool_flags"
        )
        if yaml_nodepool_flags:
            parts.append(
                "--gke_additional_nodepool_flags=" + ",".join(yaml_nodepool_flags)
            )

    if "teardown" in stage and args.network:
        parts.append("--gce_network_name=" + args.network)

    if extra_flags:
        parts.append(extra_flags)
    if passthrough_flags:
        parts.extend(passthrough_flags)

    cmd = " ".join(parts)
    return run_cmd(cmd, check=check)


def get_results_path(benchmark, variant):
    uri = get_variant_uri(benchmark, variant)
    return os.path.join(
        RESULTS_BASE, benchmark, "runs", uri, "perfkitbenchmarker_results.json"
    )


def count_result_lines(benchmark, variant):
    """Line count of the results NDJSON, used to isolate one run's output.

    PKB appends to a single file per run_uri (--json_write_mode=a), so records
    from earlier probes at the same density are still present. Callers snapshot
    this before a run and pass it as start_line so a crashed run reads as
    "no metrics" instead of silently inheriting an earlier probe's value.
    """
    path = get_results_path(benchmark, variant)
    if not os.path.exists(path):
        return 0
    with open(path, "r") as f:
        return sum(1 for _ in f)


def get_metric_from_results(
    benchmark, variant, sweep_label, sweep_val, target_metric, start_line=0
):
    """Parse the PKB JSON output to find the metric and return (value, all_found_metrics)."""
    results_file = get_results_path(benchmark, variant)

    if not os.path.exists(results_file):
        return None, []

    latest_val = None
    all_metrics = set()

    with open(results_file, "r") as f:
        for _ in range(start_line):
            if f.readline() == "":
                break
        for line in f:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                labels_str = record.get("labels", "")
                labels = {}
                for part in labels_str.split(","):
                    part = part.strip().strip("|")
                    if ":" in part:
                        k, _, v = part.partition(":")
                        labels[k.strip()] = v.strip()

                record_sweep_val = labels.get(sweep_label)
                if record_sweep_val is not None:
                    try:
                        record_sweep_val = float(record_sweep_val)
                    except ValueError:
                        pass

                    if record_sweep_val == float(sweep_val):
                        metric_name = record.get("metric")
                        if metric_name:
                            all_metrics.add(metric_name)
                            if metric_name == target_metric:
                                latest_val = record.get("value")
            except json.JSONDecodeError:
                continue

    return latest_val, sorted(list(all_metrics))


# ============================================================
# Stage Handlers
# ============================================================
def do_provision(benchmark, variant, args, passthrough_flags=None):
    logger.info("=== PROVISION: %s / %s ===", benchmark, variant)
    run_pkb(benchmark, variant, "provision", args, passthrough_flags=passthrough_flags)


def do_prepare(benchmark, variant, args, passthrough_flags=None):
    logger.info("=== PREPARE: %s / %s ===", benchmark, variant)
    run_pkb(benchmark, variant, "prepare", args, passthrough_flags=passthrough_flags)
    apply_variant_extras(variant, args.namespace)


def do_static_sweep(benchmark, variant, sweep_values, args, passthrough_flags=None):
    bench_cfg = BENCHMARKS[benchmark]
    sweep_flag = bench_cfg["sweep_flag"]
    warmpool = bench_cfg["warmpool"]
    pod_label = bench_cfg["pod_label"]
    do_drain = bench_cfg["drain_between"]

    logger.info(
        "=== STATIC SWEEP: %s / %s -- values: %s ===", benchmark, variant, sweep_values
    )
    failed = []
    try:
        for val in sweep_values:
            logger.info("--- %s=%s ---", sweep_flag, val)
            extra = f"{sweep_flag}={val}"
            rc = run_pkb(
                benchmark,
                variant,
                "run,cleanup",
                args,
                extra_flags=extra,
                passthrough_flags=passthrough_flags,
                check=False,
            )
            if rc != 0:
                logger.warning("value=%s failed (rc=%d), continuing", val, rc)
                failed.append(val)
            if do_drain and warmpool:
                logger.info("Draining between sweep levels...")
                drain_warmpool(warmpool, pod_label, args.namespace)
                time.sleep(5)
    finally:
        if do_drain and warmpool:
            logger.info("Final safety drain after sweep")
            drain_warmpool(warmpool, pod_label, args.namespace)

    if failed:
        logger.warning("Failed sweep values: %s", failed)
    else:
        logger.info("All sweep values completed successfully")


def _probe_once(
    benchmark, variant, args, bench_cfg, mid, passthrough_flags
):
    """Run one measurement at `mid`. Returns (metric_val, found_metrics).

    metric_val is None when the run produced no usable measurement (crash), which
    the caller must treat differently from a measured SLA breach.
    """
    start_line = count_result_lines(benchmark, variant)
    run_pkb(
        benchmark,
        variant,
        "run,cleanup",
        args,
        extra_flags=f"{bench_cfg['sweep_flag']}={mid}",
        passthrough_flags=passthrough_flags,
        check=False,
    )

    if bench_cfg["drain_between"] and bench_cfg["warmpool"]:
        logger.info("Draining between sweep levels...")
        drain_warmpool(bench_cfg["warmpool"], bench_cfg["pod_label"], args.namespace)
        time.sleep(5)

    return get_metric_from_results(
        benchmark,
        variant,
        bench_cfg["sweep_label"],
        mid,
        args.threshold_metric,
        start_line=start_line,
    )


def evaluate_probe(
    benchmark, variant, args, bench_cfg, mid, threshold, passthrough_flags
):
    """Decide PASS/FAIL at `mid`, re-running only on failure.

    Asymmetric by design: a false FAIL is unrecoverable under bisection because it
    truncates the whole upper branch, while a false PASS only sends the search
    higher. So passes are accepted immediately and only failures pay for repeats.

    But a false PASS is NOT harmless, because the reported answer is the highest
    density that passed — so a false PASS at the boundary becomes the verdict rather
    than self-correcting. Accepting the first passing re-run therefore biases the
    reported threshold upward, by roughly one bisection step.

    `--failure-retries` is the guard: it is the number of confirming re-runs, and
    ALL of them must pass. Default 2, because one failure plus one pass is an observed
    rate of 1/2 — an interval wide enough to be uninformative about whether the
    threshold is met.

    Requiring all N to pass also makes a genuine failure *cheaper*, not dearer: the
    first non-passing confirmation ends the probe, since all-must-pass can no longer
    be met. A saturated density therefore costs 2 runs, where accept-on-any-pass
    spent 3.

    Returns (passed, decision_value, samples, note).
    """
    samples = []
    metric_val, found_metrics = _probe_once(
        benchmark, variant, args, bench_cfg, mid, passthrough_flags
    )
    samples.append(metric_val)

    if metric_val is not None and metric_val <= threshold:
        return True, metric_val, samples, ""

    if metric_val is None and not found_metrics:
        note = "crash"
    elif metric_val is None:
        # Metric absent while other metrics were emitted: usually a partially
        # failed run (aggregates suppressed, PSI still emitted), not a typo.
        note = "no-metric"
    else:
        note = "over-threshold"

    required = args.failure_retries
    confirmed = 0
    for attempt in range(1, required + 1):
        logger.info(
            "Probe at %s failed (%s). Confirmation re-run %d of %d — all must pass.",
            mid,
            note,
            attempt,
            required,
        )
        retry_val, _ = _probe_once(
            benchmark, variant, args, bench_cfg, mid, passthrough_flags
        )
        samples.append(retry_val)
        if retry_val is None or retry_val > threshold:
            # All confirmations must pass, so one miss settles it. Stop here rather
            # than spending the rest of the budget on a verdict that cannot change.
            logger.info(
                "Confirmation %d of %d did not pass (%s) — failure stands.",
                attempt,
                required,
                retry_val,
            )
            if confirmed:
                note = f"{note}-unconfirmed-{confirmed}of{required}"
            break
        confirmed += 1
        logger.info(
            "Confirmation %d of %d PASSED (%s <= %s).",
            attempt,
            required,
            retry_val,
            threshold,
        )
    else:
        if required:
            logger.info(
                "All %d confirmations passed at %s — the first result was noise.",
                required,
                mid,
            )
            return True, samples[-1], samples, f"pass-on-{required}-confirmations"

    measured = [s for s in samples if s is not None]
    decision_value = _aggregate(measured, args) if measured else None
    return False, decision_value, samples, note


def _aggregate(values, args):
    """Combine repeated measurements at one density into a decision value."""
    if len(values) == 1:
        return values[0]
    mode = args.retry_aggregate
    if mode == "median":
        return statistics.median(values)
    if mode == "mean":
        return statistics.fmean(values)
    if mode == "min":
        return min(values)
    if mode == "max":
        return max(values)
    if mode == "weighted":
        # Weights apply oldest-first and are renormalised to len(values).
        weights = [float(w) for w in args.retry_weights.split(",")]
        weights = (weights + [weights[-1]] * len(values))[: len(values)]
        total = sum(weights)
        return sum(v * w for v, w in zip(values, weights)) / total
    return values[-1]


def do_binary_search(benchmark, variant, args, passthrough_flags=None):
    bench_cfg = BENCHMARKS[benchmark]
    sweep_flag = bench_cfg["sweep_flag"]
    sweep_label = bench_cfg["sweep_label"]
    warmpool = bench_cfg["warmpool"]
    pod_label = bench_cfg["pod_label"]
    do_drain = bench_cfg["drain_between"]

    is_float = (
        "." in args.search_min
        or "." in args.search_max
        or "." in args.search_convergence
    )
    low = float(args.search_min) if is_float else int(args.search_min)
    high = float(args.search_max) if is_float else int(args.search_max)
    convergence = (
        float(args.search_convergence) if is_float else int(args.search_convergence)
    )
    target_metric = args.threshold_metric
    threshold = args.threshold_value

    if convergence <= 0 and is_float:
        sys.exit(
            "--search-convergence must be > 0 for float sweeps; a value of 0 "
            "never terminates. For integer sweeps, 0 is allowed."
        )
    elif convergence < 0:
        sys.exit("--search-convergence must be >= 0.")
    if low >= high:
        sys.exit(f"--search-min ({low}) must be < --search-max ({high}).")
    if args.failure_retries < 0:
        sys.exit("--failure-retries must be >= 0.")

    logger.info("=== BINARY SEARCH SWEEP: %s / %s ===", benchmark, variant)
    logger.info("  Range: [%s, %s], Convergence: %s", low, high, convergence)
    logger.info("  Target: %s <= %s", target_metric, threshold)
    logger.info(
        "  Confirmations to overturn a failure: %d (all must pass, aggregate=%s)",
        args.failure_retries,
        args.retry_aggregate,
    )
    if args.failure_retries == 1:
        logger.warning(
            "  --failure-retries=1: one FAIL plus one PASS will be recorded as a "
            "PASS. That is an observed rate of 1/2 (95%% CI 9-91%%) and says nothing "
            "about meeting the threshold, so the reported optimum will be biased "
            "upward. Prefer the default of 2."
        )

    best_val = None
    lowest_fail = None
    search_history = []
    step_num = 1

    try:
        while (high - low) >= convergence:
            mid = low + (high - low) / 2.0
            if not is_float:
                mid = int(mid)

            current_range = f"[{low}, {high}]"
            logger.info(
                "--- Binary Search Step %d: Testing %s=%s (Range: %s) ---",
                step_num,
                sweep_flag,
                mid,
                current_range,
            )

            passed, decision_value, samples, note = evaluate_probe(
                benchmark,
                variant,
                args,
                bench_cfg,
                mid,
                threshold,
                passthrough_flags,
            )

            metric_val_str = "N/A" if decision_value is None else str(decision_value)
            if len(samples) > 1:
                metric_val_str += (
                    " (" + ", ".join("N/A" if s is None else f"{s:g}" for s in samples) + ")"
                )

            if passed:
                logger.info(
                    "Density %s PASSED (%s <= %s). Searching higher.",
                    mid,
                    decision_value,
                    threshold,
                )
                status = "Success" if not note else f"Success ({note})"
                best_val = mid
                if is_float:
                    low = mid
                else:
                    low = mid + 1
            else:
                if note == "crash":
                    logger.warning(
                        "Density %s produced no metrics after %d attempt(s) — "
                        "treating as saturation. NOTE: an infrastructure failure is "
                        "indistinguishable from saturation here; check the run log.",
                        mid,
                        len(samples),
                    )
                    status = "Crashed/No Data"
                elif note == "no-metric":
                    logger.warning(
                        "Metric '%s' absent at density %s though other metrics were "
                        "emitted — partial run failure. Treating as saturation. If this "
                        "occurs at EVERY density, check --threshold-metric for a typo.",
                        target_metric,
                        mid,
                    )
                    status = "Partial/No Metric"
                else:
                    logger.info(
                        "Density %s FAILED (%s > %s). Searching lower.",
                        mid,
                        decision_value,
                        threshold,
                    )
                    status = "Failed SLA"
                lowest_fail = mid if lowest_fail is None else min(lowest_fail, mid)
                if is_float:
                    high = mid
                else:
                    high = mid - 1

            search_history.append(
                {
                    "Step": step_num,
                    "Search Range": current_range,
                    f"Target {sweep_label}": mid,
                    target_metric: metric_val_str,
                    "Threshold": threshold,
                    "Status": status,
                }
            )

            step_num += 1

            if not is_float and low > high:
                break

    finally:
        if do_drain and warmpool:
            logger.info("Final safety drain after sweep")
            drain_warmpool(warmpool, pod_label, args.namespace)

    # Output Binary Search History Table to STDOUT
    if search_history:
        headers = [
            "Step",
            "Search Range",
            f"Target {sweep_label}",
            target_metric,
            "Threshold",
            "Status",
        ]

        # Calculate column widths
        col_widths = {h: len(h) for h in headers}
        for row in search_history:
            for h in headers:
                col_widths[h] = max(col_widths[h], len(str(row[h])))

        def format_row(row_dict):
            return (
                "| "
                + " | ".join(str(row_dict[h]).ljust(col_widths[h]) for h in headers)
                + " |"
            )

        header_str = format_row({h: h for h in headers})
        separator_str = "|" + "|".join("-" * (col_widths[h] + 2) for h in headers) + "|"

        print("\n" + "=" * len(header_str))
        print("BINARY SEARCH HISTORY")
        print("=" * len(header_str))
        print(header_str)
        print(separator_str)
        for row in search_history:
            print(format_row(row))
        print("=" * len(header_str) + "\n")

    # Persist the search history next to the results so a verdict can be audited
    # after the fact; previously it existed only on stdout.
    try:
        history_path = os.path.join(
            os.path.dirname(get_results_path(benchmark, variant)),
            "search_history.json",
        )
        with open(history_path, "w") as f:
            json.dump(
                {
                    "benchmark": benchmark,
                    "variant": variant,
                    "threshold_metric": target_metric,
                    "threshold_value": threshold,
                    "search_min": args.search_min,
                    "search_max": args.search_max,
                    "convergence": convergence,
                    "failure_retries": args.failure_retries,
                    "retry_aggregate": args.retry_aggregate,
                    "optimal": best_val,
                    "lowest_failing": lowest_fail,
                    "steps": search_history,
                },
                f,
                indent=2,
            )
        logger.info("Search history written to %s", history_path)
    except OSError as e:
        logger.warning("Could not write search history: %s", e)

    if best_val is not None:
        # Report the bracket, not just the point estimate: the true threshold lies
        # between the highest pass and the lowest fail, and is only resolved to
        # within `convergence`.
        if lowest_fail is not None:
            logger.info(
                "=== BINARY SEARCH COMPLETE: Optimal %s = %s "
                "(highest passing; lowest failing = %s; true threshold in [%s, %s), "
                "resolved to +/-%s) ===",
                sweep_label,
                best_val,
                lowest_fail,
                best_val,
                lowest_fail,
                convergence,
            )
        else:
            logger.info(
                "=== BINARY SEARCH COMPLETE: Optimal %s = %s "
                "(no failure observed — the true threshold is at or above "
                "--search-max=%s, so this is a LOWER BOUND, not a ceiling) ===",
                sweep_label,
                best_val,
                args.search_max,
            )
    else:
        logger.warning(
            "=== BINARY SEARCH COMPLETE: No value satisfied the threshold. "
            "Every probe failed, including the lowest (%s) — the true threshold is "
            "at or below --search-min, so widen the range downward. ===",
            args.search_min,
        )


def do_teardown(benchmark, variant, args, passthrough_flags=None):
    logger.info("=== TEARDOWN: %s / %s ===", benchmark, variant)
    run_pkb(benchmark, variant, "teardown", args, passthrough_flags=passthrough_flags)


def apply_variant_extras(variant, namespace):
    if variant == "overlay_none":
        logger.info("Patching SandboxTemplate: overlay2=none")
        patch_json = '{"spec":{"podTemplate":{"metadata":{"annotations":{"dev.gvisor.spec.overlay2":"none"}}}}}'
        run_cmd(
            f"kubectl patch sandboxtemplate python-sandbox-template -n {namespace} --type=merge -p '{patch_json}'",
            check=False,
        )
        time.sleep(10)

    if variant == "sched_tuning":
        logger.info("Applying scheduler tuning DaemonSet")
        j2_path = os.path.join(TUNING_DIR, "sched_tuner_daemonset.yaml.j2")
        with open(j2_path, "r") as f:
            rendered = Jinja2Template(f.read()).render(ns=namespace)
        rendered_path = os.path.join(TUNING_DIR, "tmp", "sched_tuner_daemonset.yaml")
        os.makedirs(os.path.dirname(rendered_path), exist_ok=True)
        with open(rendered_path, "w") as f:
            f.write(rendered)
        run_cmd("kubectl apply -f " + rendered_path, check=False)
        run_cmd(
            f"kubectl rollout status daemonset/sched-tuner -n {namespace} --timeout=60s",
            check=False,
        )
        time.sleep(5)


# ============================================================
# Main
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="Optimization Sweep Runner - Multi-Benchmark",
        epilog="Pass additional PKB flags after --: sweep.py ... -- --flag1=val1",
    )
    parser.add_argument("--project", required=True, help="GCP Project ID")
    parser.add_argument("--region", default="us-east1", help="GCP Region")
    parser.add_argument("--owner", default=os.environ.get("USER", "default"), help="Owner tag for resources")
    parser.add_argument("--network", default=None, help="Existing VPC network name")
    parser.add_argument("--subnet", default=None, help="Existing Subnet name")
    parser.add_argument("--namespace", default="agentic", help="Kubernetes namespace")
    parser.add_argument(
        "--benchmark",
        required=True,
        choices=sorted(BENCHMARKS.keys()),
        help="PKB benchmark name",
    )
    parser.add_argument("--variant", action="append", default=[], help="Variant to run")
    parser.add_argument("--all", action="store_true", help="Run all variants.")
    parser.add_argument(
        "--stages",
        default="provision,prepare,run,teardown",
        help="Comma-separated stages.",
    )

    # Static Sweep
    parser.add_argument(
        "--sweep-values",
        default=None,
        help="Comma-separated sweep values (Static mode).",
    )

    # Binary Search Sweep
    parser.add_argument(
        "--search-mode",
        choices=["static", "binary"],
        default="static",
        help="Sweep mode (static or binary).",
    )
    parser.add_argument("--search-min", default="1", help="Binary search min value.")
    parser.add_argument("--search-max", default="100", help="Binary search max value.")
    parser.add_argument(
        "--search-convergence", default="1", help="Binary search convergence/step size."
    )
    parser.add_argument(
        "--threshold-metric",
        default="k8s_python_density_sandbox_total_cel_mean_ms",
        help="Metric to evaluate for binary search.",
    )
    parser.add_argument(
        "--threshold-value",
        type=float,
        default=2000.0,
        help="Maximum acceptable value for the metric.",
    )
    parser.add_argument(
        "--failure-retries",
        type=int,
        default=2,
        help=(
            "Number of confirming re-runs on a FAILED probe. ALL of them must pass "
            "for the failure to be overturned. Default 2; use 0 for single-shot, "
            "where a failure is final. A first-sample pass is always accepted "
            "immediately, so a clean sweep costs nothing extra. Why all-must-pass: "
            "the reported optimum is the highest passing density, so accepting the "
            "first passing re-run lets one noisy sample at the boundary become the "
            "verdict rather than self-correcting, biasing the result upward by about "
            "one bisection step. All-must-pass also makes a real failure cheaper — "
            "the first non-passing confirmation ends the probe."
        ),
    )
    parser.add_argument(
        "--retry-aggregate",
        default="median",
        choices=["median", "mean", "min", "max", "weighted", "last"],
        help=(
            "How to combine repeated measurements at one density into the reported "
            "value once all retries have failed. Note this only affects reporting: "
            "any single passing re-run already short-circuits to PASS."
        ),
    )
    parser.add_argument(
        "--retry-weights",
        default="0.2,0.8",
        help=(
            "Comma-separated weights for --retry-aggregate=weighted, applied "
            "oldest-measurement-first and renormalised (e.g. '0.2,0.8' trusts the "
            "re-run over the initial measurement)."
        ),
    )

    args, passthrough = parser.parse_known_args()
    passthrough_flags = [f for f in passthrough if f != "--"] if passthrough else None

    benchmark = args.benchmark
    bench_cfg = BENCHMARKS[benchmark]

    if getattr(args, "all"):
        variants = list(ALL_VARIANTS)
    elif args.variant:
        variants = args.variant
    else:
        logger.error("Specify --variant <name> or --all")
        sys.exit(1)

    for v in variants:
        if v not in VARIANT_CONFIGS:
            logger.error(
                "Unknown variant: %s. Available: %s", v, ", ".join(ALL_VARIANTS)
            )
            sys.exit(1)

    stages = [s.strip() for s in args.stages.split(",")]
    valid_stages = {"provision", "prepare", "run", "teardown"}
    for s in stages:
        if s not in valid_stages:
            logger.error("Unknown stage: %s", s)
            sys.exit(1)

    logger.info("OPTIMIZATION SWEEP")
    logger.info("  Project:    %s", args.project)
    logger.info("  Region:     %s", args.region)
    logger.info("  Network:    %s", args.network or "(PKB Managed)")
    logger.info("  Subnet:     %s", args.subnet or "(PKB Managed)")
    logger.info("  Namespace:  %s", args.namespace)
    logger.info("  Benchmark:  %s", benchmark)
    logger.info("  Sweep flag: %s", bench_cfg["sweep_flag"])
    logger.info("  Variants:   %s", variants)
    logger.info("  Stages:     %s", stages)
    logger.info("  Mode:       %s", args.search_mode)

    for variant in variants:
        logger.info("")
        logger.info("=" * 60)
        logger.info("  %s / %s: %s", benchmark, variant, VARIANT_DESC.get(variant, ""))
        logger.info("=" * 60)

        if "provision" in stages:
            do_provision(benchmark, variant, args, passthrough_flags)
        if "prepare" in stages:
            do_prepare(benchmark, variant, args, passthrough_flags)
        if "run" in stages:
            if args.search_mode == "binary":
                do_binary_search(benchmark, variant, args, passthrough_flags)
            else:
                if args.sweep_values:
                    raw = [v.strip() for v in args.sweep_values.split(",")]
                    sweep_values = [float(v) if "." in v else int(v) for v in raw]
                else:
                    sweep_values = list(bench_cfg["default_sweep"])
                do_static_sweep(
                    benchmark, variant, sweep_values, args, passthrough_flags
                )
        if "teardown" in stages:
            do_teardown(benchmark, variant, args, passthrough_flags)

        logger.info("=== %s / %s COMPLETE ===", benchmark, variant)

    logger.info("ALL DONE. Results in: %s/", os.path.join(RESULTS_BASE, benchmark))


if __name__ == "__main__":
    main()
