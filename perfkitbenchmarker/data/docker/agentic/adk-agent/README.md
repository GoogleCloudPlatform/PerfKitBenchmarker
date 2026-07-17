# ADK Agent Container

FastAPI service deployed inside the GKE cluster that serves as the
orchestrator for Agent Sandbox benchmarks. PKB benchmark modules
communicate with this service via HTTP to trigger benchmark runs.

## Architecture

```
PKB (host machine)                    GKE Cluster
-----------------                     -----------
benchmark.Run()
  -> HTTP POST /benchmark/...  -->  main.py (FastAPI)
                                      -> Runner(agent=root_agent)
                                        -> MockLlm yields code
                                        -> BenchmarkGkeCodeExecutor
                                          -> SandboxClient.create_sandbox()
                                          -> sandbox.files.write("script.py", code)
                                          -> sandbox.commands.run("python3 script.py")
                                          -> SandboxClient.delete_sandbox()
```

## Components

| File | Purpose |
|------|---------|
| `main.py` | FastAPI service with benchmark endpoints |
| `gke_performance_agent/agent.py` | ADK agent definition with MockLlm and BenchmarkGkeCodeExecutor |
| `sandboxed_apps/python_test_app/benchmark_density.py` | Python density benchmark (runs inside gVisor sandbox) |
| `sandboxed_apps/python_test_app/benchmark_payload.py` | Payload transfer benchmark (runs inside gVisor sandbox) |
| `sandboxed_apps/python_test_app/benchmark_qps.py` | QPS validation script (runs inside gVisor sandbox) |

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/healthz` | Liveness probe |
| POST | `/benchmark/python/density` | Python sandbox density measurement |
| POST | `/benchmark/python/payload` | Payload transfer measurement |
| POST | `/benchmark/python/qps` | QPS saturation measurement |
| POST | `/benchmark/chromium/density` | Chromium browser density measurement |
| POST | `/run` | Raw ADK agent interaction |

## Building

The image is built automatically by PKB during the provision stage via
`container_specs`. For ARM64, use the `cloudbuild-arm64.yaml` config
with `gke_prerequisites.py --target_arch=arm64`.

## Dependencies

See `requirements.txt`. Key packages:
- `google-adk[gke,extensions]` -- Google Agent Development Kit
- `k8s-agent-sandbox` -- Sandbox lifecycle client
- `fastapi` + `uvicorn` -- HTTP service
- `playwright` -- Chromium CDP automation (orchestrator-side)
