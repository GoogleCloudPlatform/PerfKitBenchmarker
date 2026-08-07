# Kubernetes Benchmark Agent API

## Endpoints

### `GET /healthz`
Liveness probe.

### `POST /benchmark/python/density`
Runs the Python density benchmark.
**Request:**
```json
{
  "sample_count": 100,
  "sample_warmup": 5,
  "concurrent_sessions": 1,
  "sandbox_exec_timeout_s": 60
}
```
### `POST /benchmark/python/payload`
Runs the payload transfer benchmark.
**Request:**
```json
{
  "payload_size_mb": 1.0,
  "payload_iterations": 20,
  "concurrent_sessions": 1,
  "sandbox_exec_timeout_s": 60
}
```
### `POST /benchmark/python/qps`
Runs the QPS saturation benchmark.
**Request:**

{
  "target_qps": 10.0,
  "duration_s": 60.0,
  "sandbox_exec_timeout_s": 30
}
### `POST /benchmark/chromium/density`
Runs the Chromium density benchmark.
**Request:**

{
  "task_count": 10,
  "warmup_tasks": 2,
  "concurrent_sessions": 1,
  "sandbox_exec_timeout_s": 120
}
### `POST /run`
Raw ADK agent interaction.
**Request:**

{
  "prompt": "start"
}
