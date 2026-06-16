// Agentic Chromium Sandbox Benchmark (UC-C)
// Measures: Interaction Latency, Screenshot Generation, DOM Evaluation, RSS
// Requires: Playwright (pre-installed in the container image)
//
// Self-contained — no external Mock LLM service needed.  Uses data: URLs
// and inline HTML to avoid network dependencies so the benchmark measures
// pure gVisor + Chromium overhead.
//
// Environment variables (injected by orchestrator):
//   TASK_COUNT    — iterations per run (default: 10)
//   WARMUP_TASKS  — warmup iterations excluded from stats (default: 2)

const { chromium } = require('playwright');
const os = require('os');

const TASK_COUNT = parseInt(process.env.TASK_COUNT || '10');
const WARMUP_TASKS = parseInt(process.env.WARMUP_TASKS || '2');

// Inline HTML page — avoids network round-trips so we measure pure
// browser engine + gVisor overhead.
const TEST_PAGE = `data:text/html,
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
</html>`;

function percentile(sorted, p) {
  if (!sorted.length) return null;
  const idx = Math.min(Math.floor(sorted.length * p), sorted.length - 1);
  return sorted[idx];
}

function getMemoryMB() {
  try {
    const usage = process.memoryUsage();
    return {
      rss_mb: Math.round(usage.rss / 1024 / 1024 * 100) / 100,
      heap_used_mb: Math.round(usage.heapUsed / 1024 / 1024 * 100) / 100,
      heap_total_mb: Math.round(usage.heapTotal / 1024 / 1024 * 100) / 100,
    };
  } catch (e) {
    return { rss_mb: null, heap_used_mb: null, heap_total_mb: null };
  }
}

async function runBenchmark() {
  const memStart = getMemoryMB();

  // ── Cold Start: browser launch ──
  const coldStart = performance.now();
  const browser = await chromium.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-gpu',
      '--disable-dev-shm-usage',
      '--disable-async-dns',
      '--single-process',
    ],
  });
  const cold_start_ms = performance.now() - coldStart;

  const context = await browser.newContext();
  const page = await context.newPage();

  // Navigate once before the loop — amortize first-navigation overhead
  await page.goto(TEST_PAGE, { waitUntil: 'domcontentloaded' });

  // Per-task latency arrays (filled during measured runs only)
  const navigate_ms = [];
  const screenshot_ms = [];
  const evaluate_ms = [];
  const click_ms = [];
  const fill_ms = [];
  const interaction_ms = []; // all task types pooled

  for (let run = 0; run < WARMUP_TASKS + TASK_COUNT; run++) {
    const measuring = run >= WARMUP_TASKS;

    // 1. Navigate (reload the data: page)
    let t0 = performance.now();
    await page.goto(TEST_PAGE, { waitUntil: 'domcontentloaded' });
    let elapsed = performance.now() - t0;
    if (measuring) { navigate_ms.push(elapsed); interaction_ms.push(elapsed); }

    // 2. DOM evaluate — read heading text
    t0 = performance.now();
    await page.evaluate(() => document.getElementById('heading').textContent);
    elapsed = performance.now() - t0;
    if (measuring) { evaluate_ms.push(elapsed); interaction_ms.push(elapsed); }

    // 3. Fill input
    t0 = performance.now();
    await page.fill('#search', `query-${run}`);
    elapsed = performance.now() - t0;
    if (measuring) { fill_ms.push(elapsed); interaction_ms.push(elapsed); }

    // 4. Click button
    t0 = performance.now();
    await page.click('#btn');
    elapsed = performance.now() - t0;
    if (measuring) { click_ms.push(elapsed); interaction_ms.push(elapsed); }

    // 5. Verify click effect (DOM mutation)
    t0 = performance.now();
    await page.evaluate(() => document.getElementById('output').textContent);
    elapsed = performance.now() - t0;
    if (measuring) { evaluate_ms.push(elapsed); interaction_ms.push(elapsed); }

    // 6. Screenshot (snapshot generation)
    t0 = performance.now();
    await page.screenshot({ path: '/tmp/snap.png' });
    elapsed = performance.now() - t0;
    if (measuring) { screenshot_ms.push(elapsed); interaction_ms.push(elapsed); }
  }

  await browser.close();
  const memEnd = getMemoryMB();

  // ── Compute stats ──
  const computeStats = (arr) => {
    if (!arr.length) return null;
    const sorted = [...arr].sort((a, b) => a - b);
    const sum = sorted.reduce((a, b) => a + b, 0);
    return {
      mean_ms: Math.round(sum / sorted.length * 1000) / 1000,
      p50_ms: Math.round(percentile(sorted, 0.50) * 1000) / 1000,
      p95_ms: Math.round(percentile(sorted, 0.95) * 1000) / 1000,
      p99_ms: Math.round(percentile(sorted, 0.99) * 1000) / 1000,
      min_ms: Math.round(sorted[0] * 1000) / 1000,
      max_ms: Math.round(sorted[sorted.length - 1] * 1000) / 1000,
    };
  };

  const summary = {
    sandbox_status: 'ok',
    cold_start_ms: Math.round(cold_start_ms * 1000) / 1000,
    task_count: TASK_COUNT,
    warmup_tasks: WARMUP_TASKS,
    // Per-task-type latency stats
    navigate: computeStats(navigate_ms),
    evaluate: computeStats(evaluate_ms),
    fill: computeStats(fill_ms),
    click: computeStats(click_ms),
    screenshot: computeStats(screenshot_ms),
    // Pooled interaction latency (all types)
    interaction: computeStats(interaction_ms),
    // Memory
    rss_start_mb: memStart.rss_mb,
    rss_end_mb: memEnd.rss_mb,
    rss_growth_mb: memEnd.rss_mb != null && memStart.rss_mb != null
      ? Math.round((memEnd.rss_mb - memStart.rss_mb) * 100) / 100
      : null,
  };

  // Print JSON to stdout — orchestrator parses this
  console.log(JSON.stringify(summary));
}

runBenchmark().catch((e) => {
  console.log(JSON.stringify({
    sandbox_status: 'error',
    error: `${e.name}: ${e.message}`,
  }));
  process.exit(1);
});
