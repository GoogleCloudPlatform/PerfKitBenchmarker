#!/usr/bin/env python3
"""Optimization Sweep Runner - Multi-Benchmark.

Runs baseline + variant benchmarks for any GKE agentic benchmark.
Drains warm pool between sweep levels for clean measurements.

Usage:
  python sweep_optimization.py --benchmark k8s_python_density --variant baseline --stages run --sweep-values 1,4,8,16
  python sweep_optimization.py --benchmark k8s_payload --variant baseline --stages run --sweep-values 0.01,0.1,1,5,10
  python sweep_optimization.py --benchmark k8s_python_density --variant baseline --stages run \
      --sweep-values 50,100 -- --k8s_python_density_sample_count=50
  python sweep_optimization.py --benchmark k8s_python_density --all --stages provision,prepare,run,teardown
"""

import argparse
import re
import logging
import os
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
# Script lives in: perfkitbenchmarker/data/k8s_agents/config/optimization/
# Repo root has: pkb.py

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _find_repo_root():
    """Walk up from script location to find the repo root (contains pkb.py)."""
    d = SCRIPT_DIR
    for _ in range(10):
        if os.path.isfile(os.path.join(d, "pkb.py")) and os.path.isdir(os.path.join(d, "perfkitbenchmarker")):
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


# ============================================================
# Project Configuration
# ============================================================

PROJECT = "sada-gke-benchmarking2"
REGION = "us-east1"
OWNER = "george-kalisse"
NETWORK = "george-agentic-vpc"
SUBNET = "george-agentic-subnet"
NAMESPACE = "agentic"

# CONFIG_DIR points to the variant config YAML files
CONFIG_DIR = os.path.join(REPO_ROOT, "perfkitbenchmarker", "data",
                          "k8s_agents", "config", "variants")
RESULTS_BASE = os.path.join(REPO_ROOT, "results", "pkb")
TUNING_DIR = os.path.join(CONFIG_DIR, "tuning")
PKB_CMD = "pkb.py"


# ============================================================
# Benchmark Registry
# ============================================================
# sweep_flag:    PKB flag that receives the sweep value
# warmpool:      SandboxWarmPool name to drain (None = skip)
# pod_label:     Label selector for warm pool pods
# default_sweep: Default sweep values if --sweep-values not given
# drain_between: Whether to drain warm pool between sweep levels

BENCHMARKS = {
    "k8s_python_density": {
        "sweep_flag": "--k8s_python_density_concurrent_sandbox_count",
        "warmpool": "python-sandbox-warmpool",
        "pod_label": "sandbox=python-sandbox-example",
        "default_sweep": [1, 4, 8, 16, 32],
        "drain_between": True,
    },
    "k8s_chromium_density": {
        "sweep_flag": "--k8s_chromium_density_concurrent_sessions",
        "warmpool": "chromium-sandbox-warmpool",
        "pod_label": "sandbox=chromium-sandbox-example",
        "default_sweep": [1, 2, 4, 8],
        "drain_between": True,
    },
    "k8s_payload": {
        "sweep_flag": "--k8s_payload_size_mb",
        "warmpool": "python-sandbox-warmpool",
        "pod_label": "sandbox=python-sandbox-example",
        "default_sweep": [0.01, 0.1, 1, 5, 10],
        "drain_between": True,
    },
    "k8s_qps": {
        "sweep_flag": "--k8s_qps_target_qps",
        "warmpool": "python-sandbox-warmpool",
        "pod_label": "sandbox=python-sandbox-example",
        "default_sweep": [1, 5, 10, 20, 50],
        "drain_between": True,
    },
    "k8s_warmpool": {
        "sweep_flag": "--k8s_warmpool_target_replicas",
        "warmpool": "python-sandbox-warmpool",
        "pod_label": "sandbox=python-sandbox-example",
        "default_sweep": [10, 50, 100, 200, 500],
        "drain_between": True,
    },
    "k8s_deletion": {
        "sweep_flag": "--k8s_deletion_batch_size",
        "warmpool": "python-sandbox-warmpool",
        "pod_label": "sandbox=python-sandbox-example",
        "default_sweep": [10, 50, 100, 200, 500],
        "drain_between": True,
    },
    "k8s_snapshot": {
        "sweep_flag": "--k8s_snapshot_preload_mb",
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

VARIANT_CONFIGS = {
    "baseline": "baseline.yaml",
    "kubelet_pulls": "kubelet_pulls.yaml",
    "overlay_none": "overlay_none.yaml",
    "larger_vm": "larger_vm.yaml",
    "hyperdisk_200gb": "hyperdisk_200gb.yaml",
    "thp": "thp.yaml",
    "sched_tuning": "sched_tuning.yaml",
    "swap": "swap.yaml",
    "c4d_amd": "c4d_amd.yaml",
    "c4a_arm": "c4a_arm.yaml",
    "multi_node": "multi_node.yaml",
    "combined": "combined.yaml",
}

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
    try:
        return (
            urllib.request.urlopen("https://ifconfig.me", timeout=10)
            .read()
            .decode()
            .strip()
        )
    except Exception:
        return (
            urllib.request.urlopen("https://api.ipify.org", timeout=10)
            .read()
            .decode()
            .strip()
        )


def run_cmd(cmd, check=True):
    logger.info("CMD: %s", cmd)
    result = subprocess.run(cmd, shell=True)
    if check and result.returncode != 0:
        logger.error("Command failed (rc=%d)", result.returncode)
        sys.exit(1)
    return result.returncode


def drain_warmpool(warmpool_name, pod_label):
    """Drain warm pool to 0 and delete lingering claims."""
    if not warmpool_name:
        return
    logger.info("Draining warm pool %s to 0", warmpool_name)
    run_cmd(
        "kubectl patch sandboxwarmpool "
        + warmpool_name
        + " -n "
        + NAMESPACE
        + ' --type=merge -p \'{"spec":{"replicas":0}}\'',
        check=False,
    )
    run_cmd(
        "kubectl delete sandboxclaims --all -n "
        + NAMESPACE
        + " --ignore-not-found=true",
        check=False,
    )
    for _ in range(30):
        result = subprocess.run(
            "kubectl get pods -n "
            + NAMESPACE
            + " -l "
            + pod_label
            + " --no-headers 2>/dev/null | wc -l",
            shell=True,
            capture_output=True,
            text=True,
        )
        try:
            count = int(result.stdout.strip())
        except ValueError:
            count = 0
        if count == 0:
            logger.info("Warm pool drained (0 pods)")
            return
        logger.info("  Draining... %d pods remaining", count)
        time.sleep(3)
    logger.warning("Drain timeout: pods may still be terminating")


def _read_yaml_gke_flags(config_path):
    """Read gke_additional_flags from a variant YAML config file."""
    try:
        with open(config_path, "r") as f:
            data = yaml.safe_load(f)
        if not data:
            return []
        for bench_name, bench_config in data.items():
            if isinstance(bench_config, dict):
                fl = bench_config.get("flags", {})
                if isinstance(fl, dict):
                    gke_flags = fl.get("gke_additional_flags", [])
                    if isinstance(gke_flags, list):
                        return [str(f).strip() for f in gke_flags if f]
        return []
    except Exception as e:
        logger.warning(
            "Could not read gke_additional_flags from %s: %s", config_path, e
        )
        return []


def run_pkb(
    benchmark, variant, stage, extra_flags=None, passthrough_flags=None, check=True
):
    """Run PKB with the given benchmark, variant, and stage."""
    config = os.path.join(CONFIG_DIR, VARIANT_CONFIGS[variant])
    uri = re.sub(r"[^a-zA-Z0-9]", "", variant)
    results = os.path.join(RESULTS_BASE, benchmark)
    os.makedirs(results, exist_ok=True)

    parts = [
        "python " + PKB_CMD,
        "--benchmarks=" + benchmark,
        "--run_stage=" + stage,
        "--project=" + PROJECT,
        "--benchmark_config_file=" + config,
        "--run_uri=" + uri,
        "--json_write_mode=a",
        "--temp_dir=" + results,
    ]

    if "provision" in stage:
        parts.append("--owner=" + OWNER)
        parts.append("--gce_network_name=" + NETWORK)
        my_ip = get_my_ip()
        gke_flag_list = [
            "--workload-pool=" + PROJECT + ".svc.id.goog",
            "--subnetwork=" + SUBNET,
            "--enable-master-authorized-networks",
            "--master-authorized-networks=" + my_ip + "/32",
        ]
        yaml_flags = _read_yaml_gke_flags(config)
        for yf in yaml_flags:
            if yf not in gke_flag_list:
                gke_flag_list.append(yf)
        parts.append("--gke_additional_flags=" + ",".join(gke_flag_list))

    if "teardown" in stage:
        parts.append("--gce_network_name=" + NETWORK)

    if extra_flags:
        parts.append(extra_flags)

    if passthrough_flags:
        parts.extend(passthrough_flags)

    cmd = " ".join(parts)
    return run_cmd(cmd, check=check)


# ============================================================
# Stage Handlers
# ============================================================


def do_provision(benchmark, variant, passthrough_flags=None):
    logger.info("=== PROVISION: %s / %s ===", benchmark, variant)
    run_pkb(benchmark, variant, "provision", passthrough_flags=passthrough_flags)


def do_prepare(benchmark, variant, passthrough_flags=None):
    logger.info("=== PREPARE: %s / %s ===", benchmark, variant)
    run_pkb(benchmark, variant, "prepare", passthrough_flags=passthrough_flags)
    apply_variant_extras(variant)


def do_run(benchmark, variant, sweep_values, passthrough_flags=None):
    """Run the sweep across all values."""
    bench_cfg = BENCHMARKS[benchmark]
    sweep_flag = bench_cfg["sweep_flag"]
    warmpool = bench_cfg["warmpool"]
    pod_label = bench_cfg["pod_label"]
    do_drain = bench_cfg["drain_between"]

    logger.info(
        "=== SWEEP: %s / %s -- values: %s ===", benchmark, variant, sweep_values
    )
    failed = []
    try:
        for val in sweep_values:
            logger.info("--- %s=%s ---", sweep_flag, val)
            extra = sweep_flag + "=" + str(val)
            rc = run_pkb(
                benchmark,
                variant,
                "run,cleanup",
                extra_flags=extra,
                passthrough_flags=passthrough_flags,
                check=False,
            )
            if rc != 0:
                logger.warning("value=%s failed (rc=%d), continuing", val, rc)
                failed.append(val)

            if do_drain and warmpool:
                logger.info("Draining between sweep levels...")
                drain_warmpool(warmpool, pod_label)
                time.sleep(5)
    finally:
        if do_drain and warmpool:
            logger.info("Final safety drain after sweep")
            drain_warmpool(warmpool, pod_label)

    if failed:
        logger.warning("Failed sweep values: %s", failed)
    else:
        logger.info("All sweep values completed successfully")


def do_teardown(benchmark, variant, passthrough_flags=None):
    logger.info("=== TEARDOWN: %s / %s ===", benchmark, variant)
    run_pkb(benchmark, variant, "teardown", passthrough_flags=passthrough_flags)


def apply_variant_extras(variant):
    """Apply variant-specific configuration after Prepare."""
    if variant == "overlay_none":
        logger.info("Patching SandboxTemplate: overlay2=none")
        patch_json = (
            '{"spec":{"podTemplate":{"metadata":{"annotations":'
            '{"dev.gvisor.spec.overlay2":"none"}}}}}'
        )
        run_cmd(
            "kubectl patch sandboxtemplate python-sandbox-template"
            " -n " + NAMESPACE + " --type=merge -p '" + patch_json + "'",
            check=False,
        )
        time.sleep(10)

    if variant == "sched_tuning":
        logger.info("Applying scheduler tuning DaemonSet")
        j2_path = os.path.join(TUNING_DIR, "sched_tuner_daemonset.yaml.j2")
        with open(j2_path, "r") as f:
            rendered = Jinja2Template(f.read()).render(ns=NAMESPACE)
        rendered_path = os.path.join(TUNING_DIR, "tmp", "sched_tuner_daemonset.yaml")
        os.makedirs(os.path.dirname(rendered_path), exist_ok=True)
        with open(rendered_path, "w") as f:
            f.write(rendered)
        run_cmd("kubectl apply -f " + rendered_path, check=False)
        run_cmd(
            "kubectl rollout status daemonset/sched-tuner -n "
            + NAMESPACE
            + " --timeout=60s",
            check=False,
        )
        time.sleep(5)


# ============================================================
# Main
# ============================================================


def main():
    parser = argparse.ArgumentParser(
        description="Optimization Sweep Runner - Multi-Benchmark",
        epilog="Pass additional PKB flags after --:  sweep_optimization.py ... -- --flag1=val1 --flag2=val2",
    )
    parser.add_argument(
        "--benchmark",
        required=True,
        choices=sorted(BENCHMARKS.keys()),
        help="PKB benchmark name to run.",
    )
    parser.add_argument(
        "--variant",
        action="append",
        default=[],
        help="Variant to run (can specify multiple).",
    )
    parser.add_argument("--all", action="store_true", help="Run all variants.")
    parser.add_argument(
        "--stages",
        default="provision,prepare,run,teardown",
        help="Comma-separated stages (default: provision,prepare,run,teardown).",
    )
    parser.add_argument(
        "--sweep-values",
        default=None,
        help="Comma-separated sweep values. Defaults vary by benchmark.",
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
        logger.error("Available: %s", ", ".join(ALL_VARIANTS))
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
            logger.error(
                "Unknown stage: %s. Valid: %s", s, ", ".join(sorted(valid_stages))
            )
            sys.exit(1)

    if args.sweep_values:
        raw = [v.strip() for v in args.sweep_values.split(",")]
        sweep_values = [float(v) if "." in v else int(v) for v in raw]
    else:
        sweep_values = list(bench_cfg["default_sweep"])

    logger.info("OPTIMIZATION SWEEP")
    logger.info("  Benchmark:  %s", benchmark)
    logger.info("  Sweep flag: %s", bench_cfg["sweep_flag"])
    logger.info("  Variants:   %s", variants)
    logger.info("  Stages:     %s", stages)
    logger.info("  Sweep:      %s", sweep_values)
    if passthrough_flags:
        logger.info("  Passthrough: %s", passthrough_flags)
    logger.info("  Repo root:  %s", REPO_ROOT)

    for variant in variants:
        logger.info("")
        logger.info("=" * 60)
        logger.info("  %s / %s: %s", benchmark, variant, VARIANT_DESC.get(variant, ""))
        logger.info("=" * 60)

        if "provision" in stages:
            do_provision(benchmark, variant, passthrough_flags)
        if "prepare" in stages:
            do_prepare(benchmark, variant, passthrough_flags)
        if "run" in stages:
            do_run(benchmark, variant, sweep_values, passthrough_flags)
        if "teardown" in stages:
            do_teardown(benchmark, variant, passthrough_flags)

        logger.info("=== %s / %s COMPLETE ===", benchmark, variant)

    logger.info("ALL DONE. Results in: %s/", os.path.join(RESULTS_BASE, benchmark))


if __name__ == "__main__":
    main()
