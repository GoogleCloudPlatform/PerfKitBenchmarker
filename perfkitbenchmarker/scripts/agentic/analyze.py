#!/usr/bin/env python3
"""Optimization Sweep Results Analyzer.

Parses PKB NDJSON results from all variants, computes deltas vs baseline,
and generates a markdown report with tables and recommendations.

Auto-discovers all metrics from the data -- no hardcoded metric lists.
Supports all agentic benchmarks via a benchmark registry with auto-discovery
fallback for unknown benchmarks.

Usage:
    python perfkitbenchmarker/scripts/agentic/analyze.py --benchmark k8s_python_density
    python perfkitbenchmarker/scripts/agentic/analyze.py --benchmark k8s_qps --results-dir results/pkb/k8s_qps
    python perfkitbenchmarker/scripts/agentic/analyze.py --benchmark k8s_payload --output-dir reports/payload

Outputs:
    reports/<benchmark>/<benchmark>_report.md   -- Full markdown report
    reports/<benchmark>/<benchmark>_raw_data.csv -- Raw data for further analysis
"""

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from pathlib import Path


# ============================================================
# Benchmark Registry
# ============================================================

BENCHMARK_REGISTRY = {
    "k8s_python_density": {
        "sweep_label": "density",
        "metric_prefix": "k8s_python_density_",
        "verdict_metric": "k8s_python_density_sandbox_total_cel_mean",
        "description": "Python Sandbox Density (UC-B)",
    },
    "k8s_chromium_density": {
        "sweep_label": "density",
        "metric_prefix": "k8s_chromium_density_",
        "verdict_metric": "k8s_chromium_density_interaction_mean",
        "description": "Chromium Browser Density (UC-C)",
    },
    "k8s_payload": {
        "sweep_label": "payload_size_mb",
        "metric_prefix": "k8s_payload_",
        "verdict_metric": "k8s_payload_sandbox_transfer_time_mean",
        "description": "Payload Transfer (UC-D)",
    },
    "k8s_qps": {
        "sweep_label": "target_qps",
        "metric_prefix": "k8s_qps_",
        "verdict_metric": "k8s_qps_ttfe_mean",
        "description": "QPS Saturation (UC-F)",
    },
    "k8s_warmpool": {
        "sweep_label": "target_replicas",
        "metric_prefix": "k8s_warmpool_",
        "verdict_metric": "k8s_warmpool_total_time_to_ready",
        "description": "Warmpool Scale-Up (UC-E)",
    },
    "k8s_deletion": {
        "sweep_label": "batch_size",
        "metric_prefix": "k8s_deletion_",
        "verdict_metric": "k8s_deletion_total_drain_time",
        "description": "Deletion & Cleanup (UC-G)",
    },
    "k8s_snapshot": {
        "sweep_label": "preload_mb",
        "metric_prefix": "k8s_snapshot_",
        "verdict_metric": "k8s_snapshot_ttfe_p50",
        "description": "Pod Snapshot (UC-A)",
    },
}


# ============================================================
# Variant Descriptions
# ============================================================

VARIANT_DESC = {
    "baseline": "Baseline (c4-standard-8, gVisor defaults)",
    "kubelet_pulls": "Kubelet tuning (maxParallelImagePulls=5, image GC)",
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
# Parsing
# ============================================================


def parse_labels(labels_str):
    """Parse PKB pipe-delimited labels into a dict."""
    result = {}
    if not labels_str:
        return result
    for part in labels_str.split(","):
        part = part.strip().strip("|")
        if ":" in part:
            key, _, val = part.partition(":")
            result[key.strip()] = val.strip()
    return result


def short_name(metric, prefix):
    """Strip the benchmark prefix to get a short display name."""
    if metric.startswith(prefix):
        return metric[len(prefix):]
    return metric


def auto_discover_prefix(metrics):
    """Auto-discover the common metric prefix from a set of metric names.

    Finds the longest common prefix ending with underscore.
    """
    if not metrics:
        return ""
    prefix = os.path.commonprefix(list(metrics))
    last_underscore = prefix.rfind("_")
    if last_underscore > 0:
        return prefix[:last_underscore + 1]
    return prefix


def auto_discover_sweep_label(results_dir):
    """Auto-discover the sweep label by scanning NDJSON records.

    Looks at the labels field of the first benchmark metric found and
    returns the first label key that looks like a sweep parameter
    (numeric value, not 'note' or 'machine_type').
    """
    skip_labels = {"note", "machine_type", "gvisor", "namespace"}
    runs_dir = Path(results_dir) / "runs"
    if not runs_dir.is_dir():
        return "density"

    for variant_dir in sorted(runs_dir.iterdir()):
        if not variant_dir.is_dir():
            continue
        for pattern in [
            variant_dir / "perfkitbenchmarker_results.json",
        ]:
            if not pattern.is_file():
                continue
            with open(pattern, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    labels = parse_labels(record.get("labels", ""))
                    for key, val in labels.items():
                        if key in skip_labels:
                            continue
                        try:
                            float(val)
                            return key
                        except (ValueError, TypeError):
                            continue
    return "density"  # fallback


def load_variant_data(results_dir, metric_prefix, sweep_label):
    """Load all variant NDJSON files into a structured dict.

    Groups records by (sweep_val, run_id) to distinguish separate
    runs at the same sweep value; keys are suffixed with _runN.

    Returns:
        dict: {variant: {sweep_key: {metric: value}}}
    """
    all_data = {}

    runs_dir = Path(results_dir) / "runs"
    if not runs_dir.is_dir():
        print(f"  No runs/ directory found in {results_dir}")
        return all_data

    for variant_dir in sorted(runs_dir.iterdir()):
        if not variant_dir.is_dir():
            continue
        variant_name = variant_dir.name

        results_file = variant_dir / "perfkitbenchmarker_results.json"
        if not results_file.is_file():
            results_file = None

        if not results_file:
            print(f"  SKIP {variant_name}: no results file found")
            continue

        print(f"  Loading {variant_name} from {results_file}")

        # Phase 1: Group records by (sweep_val, wall_time_s)
        run_groups = defaultdict(dict)  # (sweep_val, wall_time_s) -> {metric: value}
        line_count = 0
        metric_count = 0

        with open(results_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                line_count += 1
                metric = record.get("metric", "")
                value = record.get("value")
                labels = parse_labels(record.get("labels", ""))

                if not metric.startswith(metric_prefix):
                    continue

                sweep_val = labels.get(sweep_label)
                if sweep_val is None:
                    continue

                try:
                    sweep_val = float(sweep_val)
                    if sweep_val == int(sweep_val):
                        sweep_val = int(sweep_val)
                except (ValueError, TypeError):
                    pass

                # run_id is a unique UUID prefix per Run() invocation
                rid = labels.get("run_id", "")

                group_key = (sweep_val, rid)
                if metric in run_groups[group_key]:
                    print(f"    WARNING: duplicate metric {metric} in group {group_key}")
                run_groups[group_key][metric] = value
                metric_count += 1

        if not run_groups:
            print(f"    WARNING: no benchmark metrics found ({line_count} total lines)")
            continue

        # Phase 2: Assign display keys (plain or _runN suffixed)
        sweep_val_runs = defaultdict(list)
        for (sv, wt), metrics in sorted(run_groups.items()):
            sweep_val_runs[sv].append((wt, metrics))

        variant_data = {}
        for sv, runs in sorted(sweep_val_runs.items()):
            for idx, (wt, metrics) in enumerate(runs, 1):
                key = f"{sv}_run{idx}"
                variant_data[key] = metrics

        all_data[variant_name] = variant_data
        sweep_keys = sorted(variant_data.keys(), key=lambda k: (str(k), k))
        metrics_found = set()
        for d in variant_data.values():
            metrics_found.update(d.keys())
        print(f"    {metric_count} samples, {len(metrics_found)} unique metrics, "
              f"{len(run_groups)} runs, keys: {sweep_keys}")

    return all_data

def discover_metrics(all_data):
    """Auto-discover all metrics across all variants and sweep values."""
    all_metrics = set()
    for variant_data in all_data.values():
        for sweep_data in variant_data.values():
            all_metrics.update(sweep_data.keys())
    return sorted(all_metrics)


# ============================================================
# Analysis
# ============================================================


def compute_deltas(all_data, baseline_name="baseline"):
    """Compute percentage deltas vs baseline for each variant/sweep_value/metric."""
    baseline = all_data.get(baseline_name)
    if not baseline:
        print(f"WARNING: baseline '{baseline_name}' not found in data")
        return {}

    deltas = {}
    for variant, sweep_values in all_data.items():
        if variant == baseline_name:
            continue
        deltas[variant] = {}
        for sweep_val, metrics in sweep_values.items():
            deltas[variant][sweep_val] = {}
            baseline_metrics = baseline.get(sweep_val, {})
            for metric, value in metrics.items():
                base_val = baseline_metrics.get(metric)
                if base_val is not None and base_val != 0:
                    delta_pct = ((value - base_val) / abs(base_val)) * 100
                else:
                    delta_pct = None
                deltas[variant][sweep_val][metric] = {
                    "value": value,
                    "baseline": base_val,
                    "delta_pct": delta_pct,
                }
    return deltas


def format_delta(delta_pct):
    """Format delta percentage with direction indicator."""
    if delta_pct is None:
        return "N/A"
    sign = "+" if delta_pct > 0 else ""
    if delta_pct < -5:
        indicator = " faster"
    elif delta_pct > 5:
        indicator = " slower"
    else:
        indicator = " (flat)"
    return f"{sign}{delta_pct:.1f}%{indicator}"


def format_value(value):
    """Format a metric value for display."""
    if value is None:
        return "\u2014"
    if isinstance(value, float):
        if value >= 1000:
            return f"{value:.1f}"
        return f"{value:.2f}"
    return str(value)


def format_value_with_delta(value, delta_pct):
    """Format a value with its delta percentage."""
    if value is None:
        return "\u2014"
    base = format_value(value)
    if delta_pct is not None:
        sign = "+" if delta_pct > 0 else ""
        return f"{base} ({sign}{delta_pct:.1f}%)"
    return base


# ============================================================
# Report Generation
# ============================================================


def generate_report(benchmark, description, sweep_label, metric_prefix,
                    verdict_metric, all_data, deltas, all_metrics, output_dir):
    """Generate the full markdown report."""
    os.makedirs(output_dir, exist_ok=True)

    baseline = all_data.get("baseline", {})
    sweep_vals = sorted(set(sv for v in all_data.values() for sv in v.keys()))
    variants = [v for v in sorted(all_data.keys()) if v != "baseline"]

    lines = []
    lines.append(f"# {description} Optimization \u2014 Results Report")
    lines.append("")
    lines.append("## Overview")
    lines.append("")
    lines.append(f"- **Benchmark**: {benchmark}")
    lines.append(f"- **Sweep parameter**: {sweep_label}")
    lines.append(f"- **Sweep values tested**: {sweep_vals}")
    lines.append(f"- **Variants**: {len(all_data)} ({len(variants)} + baseline)")
    lines.append(f"- **Metrics discovered**: {len(all_metrics)}")
    lines.append("")

    # Variant descriptions
    lines.append("## Variants Tested")
    lines.append("")
    lines.append("| Variant | Description |")
    lines.append("|---------|-------------|")
    lines.append(f"| baseline | {VARIANT_DESC.get('baseline', '')} |")
    for v in variants:
        lines.append(f"| {v} | {VARIANT_DESC.get(v, '')} |")
    lines.append("")

    # Baseline absolute values
    lines.append("## Baseline Absolute Values")
    lines.append("")

    baseline_metrics = [m for m in all_metrics if any(m in baseline.get(sv, {}) for sv in sweep_vals)]
    if baseline_metrics:
        header = "| Metric | " + " | ".join(f"{sweep_label}={sv}" for sv in sweep_vals) + " |"
        sep = "|--------|" + "|".join("------:" for _ in sweep_vals) + "|"
        lines.append(header)
        lines.append(sep)
        for metric in baseline_metrics:
            row = f"| {short_name(metric, metric_prefix)} |"
            for sv in sweep_vals:
                val = baseline.get(sv, {}).get(metric)
                row += f" {format_value(val)} |"
            lines.append(row)
        lines.append("")

    # Summary comparison
    lines.append("## Summary: Delta vs Baseline")
    lines.append("")
    lines.append("Percentage change vs baseline. Negative = faster/less (better for latency metrics).")
    lines.append("Threshold: \u00b15% considered significant.")
    lines.append("")

    for sv in sweep_vals:
        lines.append(f"### {sweep_label} = {sv}")
        lines.append("")

        active_variants = [v for v in variants if sv in all_data.get(v, {})]
        if not active_variants:
            lines.append("*No variant data at this sweep value.*")
            lines.append("")
            continue

        available_metrics = [m for m in all_metrics
                           if any(m in deltas.get(v, {}).get(sv, {}) for v in active_variants)]

        if not available_metrics:
            lines.append("*No metrics available.*")
            lines.append("")
            continue

        header = "| Metric | " + " | ".join(active_variants) + " |"
        sep = "|--------|" + "|".join("------:" for _ in active_variants) + "|"
        lines.append(header)
        lines.append(sep)

        for metric in available_metrics:
            row = f"| {short_name(metric, metric_prefix)} |"
            for v in active_variants:
                d_info = deltas.get(v, {}).get(sv, {}).get(metric, {})
                delta = d_info.get("delta_pct")
                row += f" {format_delta(delta)} |"
            lines.append(row)
        lines.append("")

    # Per-variant detailed analysis
    lines.append("## Per-Variant Detailed Analysis")
    lines.append("")

    for variant in variants:
        v_data = all_data.get(variant, {})
        v_deltas = deltas.get(variant, {})
        if not v_data:
            continue

        lines.append(f"### {variant}: {VARIANT_DESC.get(variant, '')}")
        lines.append("")

        v_sweep_vals = sorted(v_data.keys())
        available_metrics = [m for m in all_metrics
                           if any(m in v_data.get(sv, {}) for sv in v_sweep_vals)]

        if not available_metrics:
            lines.append("*No metrics available.*")
            lines.append("")
            continue

        header = "| Metric | " + " | ".join(f"{sweep_label}={sv}" for sv in v_sweep_vals) + " |"
        sep = "|--------|" + "|".join("------:" for _ in v_sweep_vals) + "|"
        lines.append(header)
        lines.append(sep)

        for metric in available_metrics:
            row = f"| {short_name(metric, metric_prefix)} |"
            for sv in v_sweep_vals:
                val = v_data.get(sv, {}).get(metric)
                d_info = v_deltas.get(sv, {}).get(metric, {})
                delta = d_info.get("delta_pct")
                row += f" {format_value_with_delta(val, delta)} |"
            lines.append(row)
        lines.append("")

    # Verdict
    lines.append("## Verdict & Recommendations")
    lines.append("")
    lines.append("### Per-Variant Verdict")
    lines.append("")
    lines.append("| Variant | Verdict | Key Finding |")
    lines.append("|---------|---------|-------------|")

    for variant in variants:
        v_deltas = deltas.get(variant, {})
        if not v_deltas:
            lines.append(f"| {variant} | \u2753 No data | \u2014 |")
            continue

        verdict_deltas = []
        for sv, metrics in v_deltas.items():
            d_info = metrics.get(verdict_metric, {})
            if d_info.get("delta_pct") is not None:
                verdict_deltas.append(d_info["delta_pct"])

        if verdict_deltas:
            avg_delta = sum(verdict_deltas) / len(verdict_deltas)
            verdict_short = short_name(verdict_metric, metric_prefix)
            if avg_delta < -5:
                verdict = "\u2705 Improvement"
                finding = f"Avg {avg_delta:.1f}% better {verdict_short}"
            elif avg_delta > 5:
                verdict = "\u274c Regression"
                finding = f"Avg {avg_delta:.1f}% worse {verdict_short}"
            else:
                verdict = "\u2796 Neutral"
                finding = f"Avg {avg_delta:.1f}% delta (within noise)"
        else:
            verdict = "\u2753 Insufficient data"
            finding = f"No {short_name(verdict_metric, metric_prefix)} data"

        lines.append(f"| {variant} | {verdict} | {finding} |")

    lines.append("")

    report_path = os.path.join(output_dir, f"{benchmark}_report.md")
    with open(report_path, "w") as f:
        f.write("\n".join(lines))
    print(f"\nReport written to: {report_path}")
    return report_path


def export_csv(benchmark, metric_prefix, sweep_label, all_data, deltas,
               all_metrics, output_dir):
    """Export raw data as CSV for further analysis."""
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, f"{benchmark}_raw_data.csv")

    rows = []
    for variant, sweep_values in sorted(all_data.items()):
        for sweep_val, metrics in sorted(sweep_values.items()):
            for metric in all_metrics:
                value = metrics.get(metric)
                if value is None:
                    continue
                d_info = deltas.get(variant, {}).get(sweep_val, {}).get(metric, {})
                rows.append({
                    "variant": variant,
                    "sweep_label": sweep_label,
                    "sweep_value": sweep_val,
                    "metric": metric,
                    "metric_short": short_name(metric, metric_prefix),
                    "value": value,
                    "baseline_value": d_info.get("baseline", ""),
                    "delta_pct": round(d_info["delta_pct"], 2) if d_info.get("delta_pct") is not None else "",
                })

    if rows:
        fieldnames = ["variant", "sweep_label", "sweep_value", "metric",
                      "metric_short", "value", "baseline_value", "delta_pct"]
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"CSV written to: {csv_path} ({len(rows)} rows)")
    return csv_path


# ============================================================
# Main
# ============================================================


def main():
    parser = argparse.ArgumentParser(description="Optimization Sweep Results Analyzer")
    parser.add_argument(
        "--benchmark", required=True,
        help="Benchmark name (e.g. k8s_python_density, k8s_qps). "
             "Used to auto-configure metric prefix, sweep label, and paths.",
    )
    parser.add_argument(
        "--results-dir", default=None,
        help="Directory containing variant subdirectories. "
             "Default: results/pkb/<benchmark>",
    )
    parser.add_argument(
        "--output-dir", default=None,
        help="Output directory for report and CSV. "
             "Default: reports/<benchmark>",
    )
    parser.add_argument(
        "--baseline", default="baseline",
        help="Name of the baseline variant (default: baseline)",
    )
    args = parser.parse_args()

    benchmark = args.benchmark
    results_dir = args.results_dir or os.path.join("results", "pkb", benchmark)
    output_dir = args.output_dir or os.path.join("reports", benchmark)

    # Look up benchmark in registry, fall back to auto-discovery
    registry = BENCHMARK_REGISTRY.get(benchmark, {})
    description = registry.get("description", benchmark)
    metric_prefix = registry.get("metric_prefix", f"{benchmark}_")
    verdict_metric = registry.get("verdict_metric", "")
    sweep_label = registry.get("sweep_label", "")

    print("=" * 60)
    print("Optimization Sweep Results Analyzer")
    print("=" * 60)
    print(f"Benchmark:    {benchmark}")
    print(f"Description:  {description}")
    print(f"Results dir:  {results_dir}")
    print(f"Output dir:   {output_dir}")
    print(f"Baseline:     {args.baseline}")
    print()

    if not os.path.isdir(results_dir):
        print(f"ERROR: Results directory not found: {results_dir}")
        sys.exit(1)

    # Auto-discover sweep label if not in registry
    if not sweep_label:
        print("Auto-discovering sweep label...")
        sweep_label = auto_discover_sweep_label(results_dir)
    print(f"Sweep label:  {sweep_label}")

    # Load data
    print("\nLoading variant data...")
    all_data = load_variant_data(results_dir, metric_prefix, sweep_label)

    if not all_data:
        # Try auto-discovering metric prefix
        print(f"No data found with prefix '{metric_prefix}'. Auto-discovering...")
        all_metrics_raw = set()
        runs_dir = Path(results_dir) / "runs"
        if runs_dir.is_dir():
          for variant_dir in sorted(runs_dir.iterdir()):
            if not variant_dir.is_dir():
                continue
            for pattern in [
                variant_dir / "perfkitbenchmarker_results.json",
            ]:
                if not pattern.is_file():
                    continue
                with open(pattern, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        m = record.get("metric", "")
                        if m:
                            all_metrics_raw.add(m)

        if all_metrics_raw:
            metric_prefix = auto_discover_prefix(all_metrics_raw)
            print(f"Auto-discovered prefix: '{metric_prefix}'")
            all_data = load_variant_data(results_dir, metric_prefix, sweep_label)

    if not all_data:
        print("ERROR: No data loaded. Check results directory.")
        sys.exit(1)

    print(f"\nLoaded {len(all_data)} variants: {sorted(all_data.keys())}")

    # Auto-discover all metrics
    all_metrics = discover_metrics(all_data)
    print(f"Discovered {len(all_metrics)} unique metrics")

    # If verdict metric not set, try to pick a reasonable default
    if not verdict_metric and all_metrics:
        for m in all_metrics:
            if "mean" in m and "wall_time" not in m:
                verdict_metric = m
                break
        if not verdict_metric:
            verdict_metric = all_metrics[0]
        print(f"Auto-selected verdict metric: {verdict_metric}")
    elif verdict_metric:
        print(f"Verdict metric: {verdict_metric}")

    # Compute deltas
    print("\nComputing deltas vs baseline...")
    deltas = compute_deltas(all_data, args.baseline)

    # Generate report
    print("\nGenerating report...")
    report_path = generate_report(
        benchmark, description, sweep_label, metric_prefix,
        verdict_metric, all_data, deltas, all_metrics, output_dir,
    )

    # Export CSV
    print("\nExporting CSV...")
    csv_path = export_csv(
        benchmark, metric_prefix, sweep_label,
        all_data, deltas, all_metrics, output_dir,
    )

    print()
    print("=" * 60)
    print("DONE")
    print("=" * 60)
    print(f"  Report: {report_path}")
    print(f"  CSV:    {csv_path}")
    print()
    print("View the report:")
    print(f"  cat {report_path}")


if __name__ == "__main__":
    main()
