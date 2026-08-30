# Kubernetes Agent Sandbox Resources

Static data files consumed by the benchmark framework during deployment
and execution. Organized into three subdirectories:

## Directory Structure

```
k8s_agents/
|-- config/                          # Benchmark and variant configurations
|   |-- agentic_benchmark_config.yaml   # Shared config with YAML anchors
|   +-- variants/                       # 11 optimization variant configs
|       |-- baseline.yaml
|       |-- c4a_arm.yaml
|       |-- ...
|       |-- variant_reference_guide.md  # Detailed variant documentation
|       +-- tuning/                     # Kubelet, scheduler, swap, THP configs
|-- manifests/                       # Jinja2 K8s manifest templates
|   |-- adk-agent.yaml.j2              # ADK Agent Deployment + RBAC
|   |-- sandbox-templates.yaml.j2      # SandboxTemplate + WarmPool (Python, Chromium)
|   |-- sandbox-router.yaml.j2         # Sandbox Router Deployment
|   |-- psi-reader.yaml.j2             # PSI Reader DaemonSet
|   |-- snapshot-crds.yaml.j2          # PodSnapshot StorageConfig + Policy
|   +-- snapshot-sandbox-template.yaml.j2  # Snapshot-specific SandboxTemplate
+-- workloads/                       # Benchmark workload scripts
    +-- startup_scripts/                 # Startup scripts for snapshot benchmarks
        |-- README.md
        |-- pip_fastapi.sh
        +-- npm_vite.sh
```

## Manifests

Jinja2 templates rendered by `gke_deploy_utils.py` during the prepare
stage. Template variables use `__ name __` syntax (double underscores
with spaces) and are substituted at render time with cluster-specific
values (namespace, image paths, project, region).

## Variant Configs

Each variant YAML contains all 7 benchmark keys with cluster and
node pool specifications. See `variants/variant_reference_guide.md`
for detailed descriptions of what each variant tests and its expected
impact on each benchmark.

## Startup Scripts

The `workloads/startup_scripts/` directory contains pluggable bash scripts used exclusively by the Pod Snapshot benchmark. They simulate an AI agent initializing a fresh environment (installing dependencies, starting servers) before a snapshot is taken. Users select which script to run via the `--k8s_snapshot_preload_mode` flag to test different memory and I/O footprints (e.g., lightweight Python vs. heavyweight Node.js).
