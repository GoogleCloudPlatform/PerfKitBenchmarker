import asyncio
import logging
import os
import time
from fastapi import APIRouter
from pydantic import BaseModel, Field
from api.utils import benchmark_lock, make_sandbox_client, percentile_stats
from playwright.async_api import async_playwright
from kubernetes import client as k8s_client, config as k8s_config

logger = logging.getLogger(__name__)
router = APIRouter()

class ChromiumBenchmarkRequest(BaseModel):
    task_count: int = Field(default=10, ge=1, description="Iterations per Chromium session")
    warmup_tasks: int = Field(default=2, ge=0, description="Warmup iterations excluded from stats")
    concurrent_sessions: int = Field(default=1, ge=1, description="Number of parallel Chromium sessions")
    sandbox_exec_timeout_s: int = Field(default=120, ge=10, description="Sandbox command execution timeout in seconds")

@router.post("/benchmark/chromium/density")
async def benchmark_chromium_density(req: ChromiumBenchmarkRequest):
    async with benchmark_lock:
        sandbox_namespace = os.getenv("AGENTIC_NAMESPACE", "agentic")
        sandbox_template = "chromium-sandbox-template"

        logger.info("Starting Chromium density benchmark (CDP): concurrent_sessions=%d task_count=%d warmup_tasks=%d", req.concurrent_sessions, req.task_count, req.warmup_tasks)

        try:
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            k8s_config.load_kube_config()
        core_v1 = k8s_client.CoreV1Api()

        test_page = """data:text/html,
<!DOCTYPE html>
<html>
<head><title>PKB Chromium Benchmark</title></head>
<body>
  <h1 id="heading">Hello Sandbox</h1>
  <input id="search" type="text" placeholder="Search..." />
  <button id="btn">Click Me</button>
  <div id="output"></div>
  <script>
    document.getElementById('btn').addEventListener('click', () => {
      document.getElementById('output').textContent = 'clicked';
    });
  </script>
</body>
</html>"""

        _metrics_semaphore = asyncio.Semaphore(5)

        async def _run_chromium_session_cdp(session_id: int) -> dict:
            sb_client = make_sandbox_client()
            sandbox = None
            t_start = time.time()
            claim_ms = 0.0
            cold_start_ms = 0.0
            try:
                t0 = time.time()
                sandbox = sb_client.create_sandbox(template=sandbox_template, namespace=sandbox_namespace)
                claim_ms = (time.time() - t0) * 1000.0

                pod_name = sandbox.get_pod_name()
                pod = core_v1.read_namespaced_pod(pod_name, sandbox_namespace)
                pod_ip = pod.status.pod_ip
                if not pod_ip:
                    raise RuntimeError(f"Pod {pod_name} has no IP assigned")

                cdp_url = f"http://{pod_ip}:9223"

                async with async_playwright() as pw:
                    browser = None
                    for attempt in range(20):
                        try:
                            browser = await pw.chromium.connect_over_cdp(cdp_url)
                            break
                        except Exception:
                            if attempt >= 19: raise
                            await asyncio.sleep(0.5)

                    cold_start_ms = (time.time() - t_start) * 1000.0
                    context = await browser.new_context()
                    page = await context.new_page()
                    await page.goto(test_page, wait_until="domcontentloaded")

                    navigate_ms, screenshot_ms, evaluate_ms, click_ms, fill_ms, interaction_ms = [], [], [], [], [], []
                    total_runs = req.warmup_tasks + req.task_count
                    for run_idx in range(total_runs):
                        measuring = run_idx >= req.warmup_tasks

                        t0 = time.time()
                        await page.goto(test_page, wait_until="domcontentloaded")
                        elapsed = (time.time() - t0) * 1000.0
                        if measuring: navigate_ms.append(elapsed); interaction_ms.append(elapsed)

                        t0 = time.time()
                        await page.evaluate("() => document.getElementById('heading').textContent")
                        elapsed = (time.time() - t0) * 1000.0
                        if measuring: evaluate_ms.append(elapsed); interaction_ms.append(elapsed)

                        t0 = time.time()
                        await page.fill("#search", f"query-{run_idx}")
                        elapsed = (time.time() - t0) * 1000.0
                        if measuring: fill_ms.append(elapsed); interaction_ms.append(elapsed)

                        t0 = time.time()
                        await page.click("#btn")
                        elapsed = (time.time() - t0) * 1000.0
                        if measuring: click_ms.append(elapsed); interaction_ms.append(elapsed)

                        t0 = time.time()
                        await page.evaluate("() => document.getElementById('output').textContent")
                        elapsed = (time.time() - t0) * 1000.0
                        if measuring: evaluate_ms.append(elapsed); interaction_ms.append(elapsed)

                        t0 = time.time()
                        await page.screenshot()
                        elapsed = (time.time() - t0) * 1000.0
                        if measuring: screenshot_ms.append(elapsed); interaction_ms.append(elapsed)

                    rss_mb = None
                    for _metrics_attempt in range(4):
                        try:
                            async with _metrics_semaphore:
                                custom_api = k8s_client.CustomObjectsApi()
                                pod_metrics = await asyncio.to_thread(
                                    custom_api.get_namespaced_custom_object,
                                    group="metrics.k8s.io", version="v1beta1", namespace=sandbox_namespace, plural="pods", name=pod_name
                                )
                            for c in pod_metrics.get("containers", []):
                                usage = c.get("usage", {}).get("memory", "")
                                if usage.endswith("Ki"): rss_mb = round(int(usage[:-2]) / 1024, 1)
                                elif usage.endswith("Mi"): rss_mb = round(float(usage[:-2]), 1)
                                elif usage.endswith("Gi"): rss_mb = round(float(usage[:-2]) * 1024, 1)
                                break
                            break
                        except k8s_client.exceptions.ApiException as e:
                            if e.status == 404 and _metrics_attempt < 3:
                                logger.info(
                                    "Metrics not yet available for %s "
                                    "(attempt %d/4, retrying in 5s)",
                                    pod_name,
                                    _metrics_attempt + 1,
                                )
                                await asyncio.sleep(5)
                                continue
                            logger.warning(
                                "Failed to read pod metrics for %s: %s",
                                pod_name, e,
                            )
                            break
                        except Exception:
                            logger.warning(
                                "Failed to read pod metrics for %s",
                                pod_name,
                                exc_info=True,
                            )
                            break

                    await browser.close()

                total_ms = (time.time() - t_start) * 1000.0

                def _compute_stats(arr):
                    if not arr: return None
                    s = sorted(arr)
                    n = len(s)
                    return {
                        "mean_ms": round(sum(s) / n, 3),
                        "p50_ms": round(s[min(int(n * 0.50), n - 1)], 3),
                        "p95_ms": round(s[min(int(n * 0.95), n - 1)], 3),
                        "p99_ms": round(s[min(int(n * 0.99), n - 1)], 3),
                        "min_ms": round(s[0], 3),
                        "max_ms": round(s[-1], 3),
                    }

                return {
                    "session_id": session_id,
                    "sandbox_status": "success",
                    "orchestrator_total_ms": round(total_ms, 3),
                    "claim_ms": round(claim_ms, 3),
                    "cold_start_ms": round(cold_start_ms, 3),
                    "rss_mb": rss_mb,
                    "navigate": _compute_stats(navigate_ms),
                    "evaluate": _compute_stats(evaluate_ms),
                    "fill": _compute_stats(fill_ms),
                    "click": _compute_stats(click_ms),
                    "screenshot": _compute_stats(screenshot_ms),
                    "interaction": _compute_stats(interaction_ms),
                }

            except Exception as e:
                total_ms = (time.time() - t_start) * 1000.0
                logger.exception("Chromium CDP session %d failed", session_id)
                return {"session_id": session_id, "orchestrator_total_ms": round(total_ms, 3), "claim_ms": round(claim_ms, 3), "error": f"{type(e).__name__}: {e}"}
            finally:
                if sandbox is not None:
                    try:
                        sb_client.delete_sandbox(sandbox.claim_name, namespace=sandbox_namespace)
                    except Exception:
                        logger.warning(
                            "Failed to delete sandbox for session %d",
                            session_id,
                            exc_info=True,
                        )

        tasks = [_run_chromium_session_cdp(i) for i in range(req.concurrent_sessions)]
        session_results = await asyncio.gather(*tasks)

    successful = [r for r in session_results if "error" not in r]
    failed = [r for r in session_results if "error" in r]
    aggregate = {}
    if successful:
        orch_times = sorted(r["orchestrator_total_ms"] for r in successful)
        aggregate.update(percentile_stats(orch_times, "orchestrator_total"))
        claim_times = sorted(r["claim_ms"] for r in successful if "claim_ms" in r)
        if claim_times: aggregate.update(percentile_stats(claim_times, "claim"))
        cold_starts = sorted(r["cold_start_ms"] for r in successful if "cold_start_ms" in r)
        if cold_starts:
            aggregate["cold_start_mean_ms"] = round(sum(cold_starts) / len(cold_starts), 3)
            aggregate["cold_start_p95_ms"] = round(cold_starts[min(int(len(cold_starts) * 0.95), len(cold_starts) - 1)], 3)
        rss_vals = sorted(r["rss_mb"] for r in successful if r.get("rss_mb") is not None)
        if rss_vals: aggregate["rss_end_mb"] = round(sum(rss_vals) / len(rss_vals), 1)

        for metric_key in ("interaction", "navigate", "evaluate", "click", "fill", "screenshot"):
            means = sorted(r[metric_key]["mean_ms"] for r in successful if isinstance(r.get(metric_key), dict) and "mean_ms" in r[metric_key])
            p95s = sorted(r[metric_key]["p95_ms"] for r in successful if isinstance(r.get(metric_key), dict) and "p95_ms" in r[metric_key])
            if means: aggregate[f"{metric_key}_mean_ms"] = round(sum(means) / len(means), 3)
            if p95s: aggregate[f"{metric_key}_p95_ms"] = round(p95s[min(int(len(p95s) * 0.95), len(p95s) - 1)], 3)

    return {
        "concurrent_sessions": req.concurrent_sessions,
        "successful_sessions": len(successful),
        "failed_sessions": len(failed),
        "aggregate": aggregate,
        "sessions": session_results,
    }
