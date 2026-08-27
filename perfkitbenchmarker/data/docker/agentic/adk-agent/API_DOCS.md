# Kubernetes Benchmark Agent API

## Endpoints

### `GET /healthz`
Liveness probe.
**Response:**
```json
{
  "status": "ok"
}
```

### `POST /benchmark/python/density`
Runs the Python density benchmark.
**Request Schema:**
* `sample_count` (integer): Sample count per sandbox session.
* `sample_warmup` (integer): Warmup iterations per sandbox session.
* `concurrent_sessions` (integer): Number of parallel sandbox sessions.
* `sandbox_exec_timeout_s` (integer): Sandbox command execution timeout in seconds.

**Request Example:**
```json
{
  "sample_count": 100,
  "sample_warmup": 5,
  "concurrent_sessions": 1,
  "sandbox_exec_timeout_s": 60
}
```
**Response Schema:**
Returns aggregate metrics and a list of session results containing `orchestrator_cel`, `sandbox_total_cel`, and RSS memory metrics.

### `POST /benchmark/python/payload`
Runs the payload transfer benchmark.
**Request Schema:**
* `payload_size_mb` (float): Payload size in MB.
* `payload_iterations` (integer): Number of transfer iterations.
* `concurrent_sessions` (integer): Number of parallel sandbox sessions.
* `sandbox_exec_timeout_s` (integer): Sandbox command execution timeout in seconds.

**Request Example:**
```json
{
  "payload_size_mb": 1.0,
  "payload_iterations": 20,
  "concurrent_sessions": 1,
  "sandbox_exec_timeout_s": 60
}
```
**Response Schema:**
Returns aggregate metrics and a list of session results containing `sandbox_transfer_time_mean_ms` and throughput metrics.

### `POST /benchmark/python/qps`
Runs the QPS saturation benchmark.
**Request Schema:**
* `target_qps` (float): Target requests per second.
* `duration_s` (float): Duration of the QPS burst in seconds.
* `sandbox_exec_timeout_s` (integer): Sandbox command execution timeout in seconds.

**Request Example:**
```json
{
  "target_qps": 10.0,
  "duration_s": 60.0,
  "sandbox_exec_timeout_s": 30
}
```
**Response Schema:**
Returns `actual_qps` achieved and aggregate metrics for `ttfe_ms` and `claim_ms`.

### `POST /benchmark/chromium/density`
Runs the Chromium density benchmark.
**Request Schema:**
* `task_count` (integer): Iterations per Chromium session.
* `warmup_tasks` (integer): Warmup iterations excluded from stats.
* `concurrent_sessions` (integer): Number of parallel Chromium sessions.
* `sandbox_exec_timeout_s` (integer): Sandbox command execution timeout in seconds.

**Request Example:**
```json
{
  "task_count": 10,
  "warmup_tasks": 2,
  "concurrent_sessions": 1,
  "sandbox_exec_timeout_s": 120
}
```
**Response Schema:**
Returns aggregate metrics and a list of session results containing interaction, navigate, evaluate, click, and screenshot latencies.

### `POST /run`
Raw ADK agent interaction.
**Request Schema:**
* `prompt` (string): The prompt to send to the agent.

**Request Example:**
```json
{
  "prompt": "(Unused now, as agent has a MockLLM)"
}
```
**Response Schema:**
```json
{
  "response": "Agent output string"
}
```