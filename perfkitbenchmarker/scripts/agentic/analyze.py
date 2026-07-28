#!/usr/bin/env python3
"""Optimization Sweep Results Analyzer.

Parses PKB NDJSON results from all variants, computes deltas vs baseline,
and generates a markdown report with tables and recommendations.

Aggregates multiple runs of the same sweep value using the Median to filter noise.
Generates a Saturation Matrix integrating PSI (Pressure Stall Information) data.
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
        "verdict_metric": "k8s_python_density_sandbox_total_cel_mean_ms",
        "description": "Python Sandbox Density",
    },
    "k8s_chromium_density": {
        "sweep_label": "density",
        "metric_prefix": "k8s_chromium_density_",
        "verdict_metric": "k8s_chromium_density_interaction_mean_ms",
        "description": "Chromium Browser Density",
    },
    "k8s_payload": {
        "sweep_label": "payload_size_mb",
        "metric_prefix": "k8s_payload_",
        "verdict_metric": "k8s_payload_sandbox_transfer_time_mean_ms",
        "description": "Payload Transfer",
    },
    "k8s_qps": {
        "sweep_label": "target_qps",
        "metric_prefix": "k8s_qps_",
        "verdict_metric": "k8s_qps_ttfe_mean_ms",
        "description": "QPS Saturation",
    },
    "k8s_warmpool": {
        "sweep_label": "target_replicas",
        "metric_prefix": "k8s_warmpool_",
        "verdict_metric": "k8s_warmpool_total_time_to_ready_s",
        "description": "Warmpool Scale-Up",
    },
    "k8s_deletion": {
        "sweep_label": "batch_size",
        "metric_prefix": "k8s_deletion_",
        "verdict_metric": "k8s_deletion_total_drain_time_s",
        "description": "Deletion & Cleanup",
    },
    "k8s_snapshot": {
        "sweep_label": "preload_mb",
        "metric_prefix": "k8s_snapshot_",
        "verdict_metric": "k8s_snapshot_ttfe_p50_s",
        "description": "Pod Snapshot",
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
    if metric.startswith(prefix):
        return metric[len(prefix):]
    return metric


def auto_discover_prefix(metrics):
    if not metrics:
        return ""
    prefix = os.path.commonprefix(list(metrics))
    last_underscore = prefix.rfind("_")
    if last_underscore > 0:
        return prefix[:last_underscore + 1]
    return prefix


def auto_discover_sweep_label(results_dir):
    skip_labels = {"note", "machine_type", "gvisor", "namespace"}
    runs_dir = Path(results_dir) / "runs"
    if not runs_dir.is_dir():
        return "density"

    for variant_dir in sorted(runs_dir.iterdir()):
        if not variant_dir.is_dir():
            continue
        for pattern in [variant_dir / "perfkitbenchmarker_results.json"]:
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
    return "density"


def load_variant_data(results_dir, metric_prefix, sweep_label, benchmark):
    all_data = {}
    import re
    bench_short = benchmark.replace("k8s_", "")[:4]
    uri_to_variant = {}
    for v in VARIANT_DESC.keys():
        clean_var_8 = re.sub(r"[^a-zA-Z0-9]", "", v)[:8]
        uri_to_variant[f"{bench_short}{clean_var_8}"] = v
        clean_var_12 = re.sub(r"[^a-zA-Z0-9]", "", v)[:12]
        if clean_var_12 not in uri_to_variant:
            uri_to_variant[clean_var_12] = v

    runs_dir = Path(results_dir) / "runs"
    if not runs_dir.is_dir():
        print(f"  No runs/ directory found in {results_dir}")
        return all_data

    for variant_dir in sorted(runs_dir.iterdir()):
        if not variant_dir.is_dir():
            continue
        raw_name = variant_dir.name
        variant_name = uri_to_variant.get(raw_name, raw_name)

        results_file = variant_dir / "perfkitbenchmarker_results.json"
        if not results_file.is_file():
            continue

        print(f"  Loading {variant_name} from {results_file}")

        run_groups = defaultdict(dict)
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

                rid = labels.get("run_id", "")
                group_key = (sweep_val, rid)
                run_groups[group_key][metric] = value
                metric_count += 1

        if not run_groups:
            print(f"    WARNING: no benchmark metrics found ({line_count} total lines)")
            continue

        sweep_val_runs = defaultdict(list)
        for (sv, rid), metrics in sorted(run_groups.items()):
            sweep_val_runs[sv].append(metrics)

        all_data[variant_name] = dict(sweep_val_runs)
        sweep_keys = sorted(all_data[variant_name].keys(), key=lambda k: (str(k), k))
        print(f"    {metric_count} samples, {len(run_groups)} runs, keys: {sweep_keys}")

    return all_data


def discover_metrics(all_data):
    all_metrics = set()
    for variant_data in all_data.values():
        for runs in variant_data.values():
            for metrics in runs:
                all_metrics.update(metrics.keys())
    return sorted(all_metrics)


def get_median(values):
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    if n % 2 == 1:
        return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / 2.0


def aggregate_runs(runs, all_metrics):
    aggregated = {}
    for metric in all_metrics:
        vals = [r[metric] for r in runs if metric in r and r[metric] is not None]
        if vals:
            aggregated[metric] = get_median(vals)
    return aggregated


def compute_deltas(all_data, all_metrics, baseline_name="baseline"):
    baseline = all_data.get(baseline_name)
    if not baseline:
        print(f"WARNING: baseline '{baseline_name}' not found in data")
        return {}, {}

    aggregated_data = {}
    for variant, sweep_values in all_data.items():
        aggregated_data[variant] = {}
        for sv, runs in sweep_values.items():
            aggregated_data[variant][sv] = aggregate_runs(runs, all_metrics)

    deltas = {}
    for variant, sweep_values in aggregated_data.items():
        if variant == baseline_name:
            continue
        deltas[variant] = {}
        for sv, metrics in sweep_values.items():
            deltas[variant][sv] = {}
            baseline_metrics = aggregated_data[baseline_name].get(sv, {})
            for metric, value in metrics.items():
                base_val = baseline_metrics.get(metric)
                if base_val is not None and base_val != 0:
                    delta_pct = ((value - base_val) / abs(base_val)) * 100
                else:
                    delta_pct = None
                deltas[variant][sv][metric] = {
                    "value": value,
                    "baseline": base_val,
                    "delta_pct": delta_pct,
                }
    return aggregated_data, deltas


def format_delta(delta_pct):
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
    if value is None:
        return "—"
    if isinstance(value, float):
        if value >= 1000:
            return f"{value:.1f}"
        return f"{value:.2f}"
    return str(value)


def format_value_with_delta(value, delta_pct):
    if value is None:
        return "—"
    base = format_value(value)
    if delta_pct is not None:
        sign = "+" if delta_pct > 0 else ""
        return f"{base} ({sign}{delta_pct:.1f}%)"
    return base


# ============================================================
# Report Generation
# ============================================================


def generate_report(benchmark, description, sweep_label, metric_prefix,
                    verdict_metric, aggregated_data, deltas, all_metrics, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    baseline = aggregated_data.get("baseline", {})
    sweep_vals = sorted(set(sv for v in aggregated_data.values() for sv in v.keys()))
    variants = [v for v in sorted(aggregated_data.keys()) if v != "baseline"]

    lines = []
    lines.append(f"# {description} Optimization — Results Report")
    lines.append("")
    lines.append("## Overview")
    lines.append("")
    lines.append(f"- **Benchmark**: {benchmark}")
    lines.append(f"- **Sweep parameter**: {sweep_label}")
    lines.append(f"- **Sweep values tested**: {sweep_vals}")
    lines.append(f"- **Variants**: {len(aggregated_data)} ({len(variants)} + baseline)")
    lines.append(f"- **Metrics discovered**: {len(all_metrics)}")
    lines.append("- **Aggregation**: Values shown are the **Median** across multiple runs to filter noise.")
    lines.append("")

    lines.append("## Variants Tested")
    lines.append("")
    lines.append("| Variant | Description |")
    lines.append("|---------|-------------|")
    lines.append(f"| baseline | {VARIANT_DESC.get('baseline', '')} |")
    for v in variants:
        lines.append(f"| {v} | {VARIANT_DESC.get(v, '')} |")
    lines.append("")

    # ============================================================
    # Saturation & Verdict Matrix (New)
    # ============================================================
    lines.append("## Saturation & Verdict Matrix")
    lines.append("")
    lines.append(f"Shows the **{short_name(verdict_metric, metric_prefix)}** delta vs baseline across the saturation curve.")
    lines.append("Includes **CPU Contention (PSI)** to identify when the node becomes CPU-starved.")
    lines.append("Format: `[Delta %] <br> *(CPU PSI: X%)*`")
    lines.append("")

    # Find the CPU PSI metric (avg10 is best for immediate contention)
    psi_metric = next((m for m in all_metrics if "psi_cpu_some_avg10" in m), None)

    header = "| Variant | " + " | ".join(f"{sweep_label}={sv}" for sv in sweep_vals) + " |"
    sep = "|--------|" + "|".join("------:" for _ in sweep_vals) + "|"
    lines.append(header)
    lines.append(sep)

    for variant in variants:
        row = f"| **{variant}** |"
        for sv in sweep_vals:
            v_deltas = deltas.get(variant, {}).get(sv, {})
            d_info = v_deltas.get(verdict_metric, {})
            delta = d_info.get("delta_pct")

            psi_val = aggregated_data.get(variant, {}).get(sv, {}).get(psi_metric) if psi_metric else None

            cell = ""
            if delta is not None:
                icon = "✅" if delta < -5 else ("❌" if delta > 5 else "➖")
                cell += f"{icon} **{delta:+.1f}%**"
            else:
                cell += "N/A"

            if psi_val is not None:
                cell += f"<br>*(CPU: {psi_val:.1f}%)*"

            row += f" {cell} |"
        lines.append(row)
    lines.append("")

    lines.append("## Baseline Absolute Values (Median)")
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

    lines.append("## Summary: Delta vs Baseline")
    lines.append("")
    lines.append("Percentage change vs baseline. Negative = faster/less (better for latency metrics).")
    lines.append("Threshold: ±5% considered significant.")
    lines.append("")

    for sv in sweep_vals:
        lines.append(f"### {sweep_label} = {sv}")
        lines.append("")

        active_variants = [v for v in variants if sv in aggregated_data.get(v, {})]
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

    lines.append("## Per-Variant Detailed Analysis")
    lines.append("")

    for variant in variants:
        v_data = aggregated_data.get(variant, {})
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

    report_path = os.path.join(output_dir, f"{benchmark}_report.md")
    with open(report_path, "w") as f:
        f.write("\n".join(lines))
    print(f"\nReport written to: {report_path}")
    return report_path


def export_csv(benchmark, metric_prefix, sweep_label, all_data, deltas,
               all_metrics, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, f"{benchmark}_raw_data.csv")

    rows = []
    for variant, sweep_values in sorted(all_data.items()):
        for sweep_val, runs in sorted(sweep_values.items()):
            for run_idx, metrics in enumerate(runs, 1):
                for metric in all_metrics:
                    value = metrics.get(metric)
                    if value is None:
                        continue
                    d_info = deltas.get(variant, {}).get(sweep_val, {}).get(metric, {})
                    rows.append({
                        "variant": variant,
                        "sweep_label": sweep_label,
                        "sweep_value": sweep_val,
                        "run_index": run_idx,
                        "metric": metric,
                        "metric_short": short_name(metric, metric_prefix),
                        "value": value,
                        "median_baseline_value": d_info.get("baseline", ""),
                        "delta_pct_vs_median": round(d_info["delta_pct"], 2) if d_info.get("delta_pct") is not None else "",
                    })

    if rows:
        fieldnames = ["variant", "sweep_label", "sweep_value", "run_index", "metric",
                      "metric_short", "value", "median_baseline_value", "delta_pct_vs_median"]
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"CSV written to: {csv_path} ({len(rows)} rows)")
    return csv_path


def main():
    parser = argparse.ArgumentParser(description="Optimization Sweep Results Analyzer")
    parser.add_argument("--benchmark", required=True)
    parser.add_argument("--results-dir", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--baseline", default="baseline")
    args = parser.parse_args()

    benchmark = args.benchmark
    results_dir = args.results_dir or os.path.join("results", "pkb", benchmark)
    output_dir = args.output_dir or os.path.join("reports", benchmark)

    registry = BENCHMARK_REGISTRY.get(benchmark, {})
    description = registry.get("description", benchmark)
    metric_prefix = registry.get("metric_prefix", f"{benchmark}_")
    verdict_metric = registry.get("verdict_metric", "")
    sweep_label = registry.get("sweep_label", "")

    print("=" * 60)
    print("Optimization Sweep Results Analyzer")
    print("=" * 60)

    if not os.path.isdir(results_dir):
        print(f"ERROR: Results directory not found: {results_dir}")
        sys.exit(1)

    if not sweep_label:
        sweep_label = auto_discover_sweep_label(results_dir)

    all_data = load_variant_data(results_dir, metric_prefix, sweep_label, benchmark)

    if not all_data:
        all_metrics_raw = set()
        runs_dir = Path(results_dir) / "runs"
        if runs_dir.is_dir():
          for variant_dir in sorted(runs_dir.iterdir()):
            if not variant_dir.is_dir():
                continue
            for pattern in [variant_dir / "perfkitbenchmarker_results.json"]:
                if not pattern.is_file():
                    continue
                with open(pattern, "r") as f:
                    for line in f:
                        if not line.strip(): continue
                        try:
                            record = json.loads(line)
                            if record.get("metric"):
                                all_metrics_raw.add(record["metric"])
                        except json.JSONDecodeError:
                            continue

        if all_metrics_raw:
            metric_prefix = auto_discover_prefix(all_metrics_raw)
            all_data = load_variant_data(results_dir, metric_prefix, sweep_label, benchmark)

    if not all_data:
        print("ERROR: No data loaded. Check results directory.")
        sys.exit(1)

    all_metrics = discover_metrics(all_data)

    if not verdict_metric and all_metrics:
        for m in all_metrics:
            if "mean" in m and "wall_time" not in m:
                verdict_metric = m
                break
        if not verdict_metric:
            verdict_metric = all_metrics[0]

    aggregated_data, deltas = compute_deltas(all_data, all_metrics, args.baseline)

    report_path = generate_report(
        benchmark, description, sweep_label, metric_prefix,
        verdict_metric, aggregated_data, deltas, all_metrics, output_dir,
    )

    csv_path = export_csv(
        benchmark, metric_prefix, sweep_label,
        all_data, deltas, all_metrics, output_dir,
    )

if __name__ == "__main__":
    main()
