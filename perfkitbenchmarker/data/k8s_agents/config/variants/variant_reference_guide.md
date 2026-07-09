# Optimization Variant Reference Guide

## Baseline Configuration

All variants are compared against this reference:

| Parameter | Baseline Value |
|-----------|---------------|
| Machine type | c4-standard-8 (4 cores / 8 vCPUs, 30 GB RAM) |
| Boot disk | hyperdisk-balanced, 100 GB (sandbox pool) |
| Runtime | gVisor (GKE Sandbox) |
| GKE version | 1.35.5-gke.1057002 |
| Platform | xemu (Google's hardware-accelerated, default) |
| Filesystem | lisafs + directfs + exclusive (all defaults) |
| Overlay | root:self (disk-backed, host page cache) |
| THP | madvise (default) |
| Swap | disabled |
| Kubelet | default settings |
| Scheduler | default kernel tunables |
| Sandbox nodes | 1 |

---

## Variant Descriptions

### var1 — Kubelet Parallel Image Pulls + Image GC

**What changes vs baseline:**
- `maxParallelImagePulls: 5` (default: 1 — serial pulls)
- `imageGcHighThresholdPercent: 70` (default: 85)
- `imageGcLowThresholdPercent: 50` (default: 80)
- Applied via `--system-config-from-file` on sandbox node pool creation

**What it tests:** Whether parallelizing container image pulls and aggressively garbage-collecting unused images improves pod creation throughput. The hypothesis is that serial image pulls bottleneck warm pool provisioning when many pods start simultaneously.

| Benchmark | Expected Influence | Rationale |
|-----------|-------------------|-----------|
| k8s_python_density | None | Measures in-sandbox execution, not pod creation |
| k8s_chromium_density | None | Same — measures browser interaction after pod is running |
| k8s_payload | None | Measures data transfer, not pod creation |
| k8s_qps | Indirect | Faster image pulls -> faster warm pool refill -> sustained QPS |
| **k8s_warmpool** | **Direct** | **Parallel pulls directly accelerate bulk pod provisioning** |
| k8s_deletion | None | Image GC may help disk cleanup, but deletion is IPAM-bound |
| k8s_snapshot | Indirect | Faster pod creation after restore, but restore is CRIU-bound |

---

### var2 — gVisor overlay2=none

**What changes vs baseline:**
- `dev.gvisor.spec.overlay2: none` (default: `root:self`)
- Applied via `kubectl patch sandboxtemplate` post-prepare
- Removes the overlay filesystem layer — sandbox writes go directly to the container's rootfs

**What it tests:** Whether removing the overlay2 filesystem layer reduces I/O overhead for file-heavy operations. The hypothesis is that overlay adds latency to every file open/stat/read through extra indirection layers.

| Benchmark | Expected Influence | Rationale |
|-----------|-------------------|-----------|
| k8s_python_density | Harmful | Import CEL worsens — loses page cache sharing |
| k8s_chromium_density | Likely harmful | Chrome loads many shared libs — same cache loss |
| k8s_payload | Possible | Stdout write path may differ, but payload is memory to stdout |
| k8s_qps | None | QPS measures scheduling, not filesystem |
| k8s_warmpool | None | Pod creation doesn't depend on overlay mode |
| k8s_deletion | None | Deletion is pod lifecycle, not filesystem |
| k8s_snapshot | Unknown | Snapshot captures filesystem state — overlay=none may reduce snapshot size but slow restore |

---

### var3 — c4-standard-16 (2x CPU)

**What changes vs baseline:**
- Machine type: `c4-standard-16` (8 cores / 16 vCPUs, 60 GB RAM)
- Both default pool and sandbox pool upgraded
- 2x CPU, 2x RAM vs baseline

**What it tests:** Whether doubling available compute resources directly reduces execution latency and raises the density ceiling. This is the "throw hardware at it" variant — establishes whether the bottleneck is CPU-bound.

| Benchmark | Expected Influence | Rationale |
|-----------|-------------------|-----------|
| **k8s_python_density** | **Strong improvement** | **CPU-bound workload directly benefits from 2x cores** |
| **k8s_chromium_density** | **Strong improvement** | **Chrome rendering + JS execution are CPU-bound** |
| **k8s_payload** | **Moderate improvement** | **base64 serialization is CPU-bound** |
| **k8s_qps** | **Moderate improvement** | **More CPU headroom for concurrent sandbox lifecycle ops** |
| **k8s_warmpool** | **Moderate improvement** | **Faster container init, more scheduling headroom** |
| k8s_deletion | Minor | Deletion is control-plane/IPAM bound, not node CPU |
| **k8s_snapshot** | **Moderate improvement** | **CRIU restore + gVisor init are CPU-intensive** |

---

### var4 — Hyperdisk-balanced 200 GB

**What changes vs baseline:**
- Sandbox pool boot disk: `hyperdisk-balanced 200 GB` (default: 100 GB)
- Larger disk = more baseline IOPS (hyperdisk scales with size)
- No change to machine type

**What it tests:** Whether increased disk I/O throughput improves pod creation speed and file-heavy operations. Hyperdisk-balanced IOPS scale with provisioned size, so 200 GB provides ~2x the IOPS of 100 GB.

| Benchmark | Expected Influence | Rationale |
|-----------|-------------------|-----------|
| k8s_python_density | None | CPU-bound workload, disk irrelevant after pod starts |
| k8s_chromium_density | Minor | Chrome writes temp files, but execution is CPU-bound |
| k8s_payload | None | Payload goes through stdout, not disk |
| k8s_qps | Minor | Pod creation involves image layer extraction (disk I/O) |
| **k8s_warmpool** | **Moderate** | **Bulk pod creation extracts image layers — disk IOPS matters** |
| k8s_deletion | Minor | Pod cleanup involves filesystem teardown |
| **k8s_snapshot** | **Direct** | **Snapshot write/restore is GCS-bound but local staging uses disk** |

---

### var5 — Transparent Hugepages (THP=always)

**What changes vs baseline:**
- `transparentHugepageEnabled: ALWAYS` (default: `madvise`)
- `transparentHugepageDefrag: DEFER` (default: varies)
- Applied via `--system-config-from-file` on sandbox node pool

**What it tests:** Whether using 2 MB huge pages instead of 4 KB pages reduces TLB (Translation Lookaside Buffer) misses for memory-intensive workloads. The hypothesis is that Python's memory allocator and gVisor's sentry benefit from fewer page table entries.

| Benchmark | Expected Influence | Rationale |
|-----------|-------------------|-----------|
| k8s_python_density | Harmful at density | Compaction stalls worsen with more pods |
| k8s_chromium_density | Likely harmful | Chrome's multi-process model amplifies compaction |
| k8s_payload | Unknown | Large memory allocations might benefit, but compaction risk |
| k8s_qps | Harmful | Compaction during pod creation adds latency |
| k8s_warmpool | Harmful | Bulk pod creation triggers heavy compaction |
| k8s_deletion | None | Deletion doesn't allocate memory |
| k8s_snapshot | Unknown | Snapshot restore allocates large memory blocks — could help or hurt |

---

### var6 — Kernel Scheduler Tuning

**What changes vs baseline:**
- `sched_migration_cost_ns: 2212926` (higher = less eager migration between cores)
- `sched_min_granularity_ns: 4656091` (larger time slices)
- `sched_wakeup_granularity_ns: 26403982` (less preemption on wakeup)
- Applied via privileged DaemonSet writing to `/proc/sys/kernel/sched_*`

**What it tests:** Whether reducing context-switch frequency improves throughput for many concurrent containers. The hypothesis is that at high density, the default scheduler switches between gVisor sentry processes too aggressively, wasting CPU on context switches.

| Benchmark | Expected Influence | Rationale |
|-----------|-------------------|-----------|
| k8s_python_density | Harmful | Larger time slices increase scheduling unfairness at high density |
| k8s_chromium_density | Likely harmful | Chrome's multi-process model needs responsive scheduling |
| k8s_payload | Neutral | Single-threaded transfer, scheduling less relevant |
| k8s_qps | Harmful | Slower preemption delays sandbox claim processing |
| k8s_warmpool | Unknown | Could help or hurt bulk pod init depending on contention pattern |
| k8s_deletion | None | Deletion is API-server/kubelet bound, not scheduler |
| k8s_snapshot | Unknown | CRIU restore is single-threaded, may not be affected |

---

### var7 — Swap Enabled

**What changes vs baseline:**
- `swapConfig.enabled: true`
- `vm.swappiness: 15` (low — only swap under memory pressure)
- Applied via `--system-config-from-file`
- Only Burstable QoS pods can use swap (Guaranteed QoS pods are excluded)

**What it tests:** Whether enabling swap allows higher pod density by offloading cold memory pages to disk. The hypothesis is that at very high density, some sandbox memory (e.g., imported-but-idle Python modules) can be swapped out to make room for more pods.

| Benchmark | Expected Influence | Rationale |
|-----------|-------------------|-----------|
| k8s_python_density | Neutral (unless memory-bound) | Only helps if density exceeds RAM capacity |
| k8s_chromium_density | **Possible** | **Chrome is memory-hungry (~100-200 MB/instance) — swap could extend density ceiling** |
| k8s_payload | None | Payload size is small relative to RAM |
| k8s_qps | None | QPS is scheduling-bound, not memory-bound |
| k8s_warmpool | Minor | More pods can fit if memory is the constraint |
| k8s_deletion | None | Deletion doesn't allocate memory |
| k8s_snapshot | Minor | Swap could allow larger preload sizes before OOM |

---

### var8 — C4D AMD (c4d-standard-8)

**What changes vs baseline:**
- Machine type: `c4d-standard-8` (AMD EPYC, same vCPU/RAM as baseline)
- Different CPU microarchitecture (AMD vs Intel)
- Same core count, same memory

**What it tests:** Whether AMD's CPU microarchitecture handles gVisor's syscall interception more efficiently than Intel's. gVisor's sentry traps every syscall via hardware virtualization extensions — AMD and Intel implement these differently (AMD-V vs VT-x).

| Benchmark | Expected Influence | Rationale |
|-----------|-------------------|-----------|
| **k8s_python_density** | **Strong improvement** | **Syscall interception is the core overhead — AMD handles it better** |
| **k8s_chromium_density** | **Strong improvement** | **Chrome makes many syscalls — same benefit** |
| **k8s_payload** | **Moderate improvement** | **stdout write is a syscall through gVisor** |
| **k8s_qps** | **Moderate improvement** | **Sandbox lifecycle involves many syscalls** |
| **k8s_warmpool** | **Minor** | **Pod creation is control-plane bound, not syscall bound** |
| k8s_deletion | Minor | Deletion is API-server bound |
| **k8s_snapshot** | **Moderate improvement** | **CRIU restore + gVisor init involve heavy syscall activity** |

---

### var9 — C4A ARM (c4a-standard-8)

**What changes vs baseline:**
- Machine type: `c4a-standard-8` (ARM Neoverse, same vCPU/RAM as baseline)
- Entirely different ISA (ARM64 vs x86_64)
- Requires ARM container images (cross-compiled via QEMU)
- `--node-architecture-taint-behavior=none` on both pools

**What it tests:** Whether ARM's instruction set architecture provides different gVisor overhead characteristics. ARM has a different syscall ABI and virtualization model — gVisor's sentry may behave differently.

| Benchmark | Expected Influence | Rationale |
|-----------|-------------------|-----------|
| **k8s_python_density** | **Unknown** | **ARM syscall ABI differs — could be better or worse** |
| **k8s_chromium_density** | **Unknown** | **Chrome on ARM is less mature — may have different perf profile** |
| k8s_payload | Unknown | ARM memory bandwidth differs |
| k8s_qps | Unknown | Scheduling overhead may differ |
| k8s_warmpool | Unknown | Pod creation speed on ARM nodes |
| k8s_deletion | Unknown | Likely similar to x86 (API-server bound) |
| k8s_snapshot | Unknown | CRIU on ARM may have different characteristics |

---

### var10 — Multi-Node Sandbox Pool (2x c4-standard-8)

**What changes vs baseline:**
- Sandbox node pool: `vm_count: 2` (default: 1)
- Same machine type per node (c4-standard-8)
- Default pool stays at 1 node
- Total sandbox capacity: 2x CPU, 2x RAM, 2x disk across 2 nodes

**What it tests:** Whether distributing sandbox pods across multiple nodes improves provisioning speed, scheduling throughput, and density ceiling. Every previous test ran on a single sandbox node — this tests whether the bottleneck is per-node resource saturation or control-plane scheduling.

Key questions this variant answers:
- Does warmpool provisioning scale linearly with nodes? (2 nodes = 2x pods/sec?)
- Does QPS saturation point double with 2 nodes?
- Does deletion/IP reclamation improve with pods spread across nodes?
- Is the GKE scheduler or the node the bottleneck at high density?

| Benchmark | Expected Influence | Rationale |
|-----------|-------------------|-----------|
| k8s_python_density | Moderate | Pods spread across 2 nodes — each node at ~half density, less CPU contention |
| **k8s_warmpool** | **Strong** | **Pod creation parallelized across 2 kubelets** |
| **k8s_qps** | **Strong** | **Warm pool spans 2 nodes — 2x capacity before drain** |
| k8s_payload | Minor | Payload transfer is per-sandbox, not affected by node count |
| k8s_chromium_density | Moderate | Chrome pods spread across nodes — more total RAM |
| **k8s_deletion** | **Moderate** | **IPAM reclamation distributed across 2 nodes** |
| **k8s_snapshot** | **Moderate** | **Snapshot/restore parallelized across nodes** |

---

### combined — Combined Best Knobs

**What changes vs baseline:**
- Machine type: `c4-standard-16` (from var3)
- `maxParallelImagePulls: 5` (from var1)
- NOTE: Currently also applies overlay2=none (var2) and sched tuning (var6) — these should be removed

**What it tests:** Whether combining individually-beneficial knobs produces compounding improvement.

---

## Expansion Plan: Which Variants to Test Per Benchmark

Based on the analysis above, here's the recommended test matrix. Only variants with a plausible positive or unknown influence are included — proven-harmful variants (var2, var5, var6) are excluded.

### Priority Matrix

| Benchmark | Priority Variants | Why |
|-----------|------------------|-----|
| **k8s_warmpool** | var1, var3, var4, var8, var10 | Pod creation speed: kubelet (var1), CPU (var3), disk IOPS (var4), AMD (var8), multi-node (var10) |
| **k8s_qps** | var3, var8, var10 | Scheduling throughput: CPU-bound; multi-node doubles capacity |
| **k8s_payload** | var3, var8 | Serialization is CPU-bound, syscall overhead matters |
| **k8s_chromium_density** | var3, var7, var8 | CPU (var3), memory ceiling (var7), AMD syscalls (var8) |
| **k8s_deletion** | var4, var10, baseline | Disk cleanup + IPAM — multi-node distributes pressure |
| **k8s_snapshot** | var3, var4, var8 | CRIU restore is CPU+disk, AMD may help |

### Variants to Skip for Non-UC-B Benchmarks

| Variant | Reason to Skip |
|---------|---------------|
| var2 (overlay2=none) | Harmful everywhere — loses page cache sharing |
| var5 (THP=always) | Harmful at density — compaction stalls |
| var6 (sched tuning) | Harmful — defaults are better for containers |

### Suggested Sweep Values Per Benchmark

| Benchmark | Sweep Parameter | Suggested Values | Rationale |
|-----------|----------------|-----------------|-----------|
| k8s_python_density | concurrent_sandbox_count | 1, 10, 50, 100, 150, 200 | Find saturation curve |
| k8s_chromium_density | concurrent_sessions | 1, 2, 4, 8, 12, 16 | Chrome is much heavier per instance |
| k8s_payload | payload_size_mb | 0.01, 0.1, 1, 5, 10, 50 | Find bandwidth saturation |
| k8s_qps | target_qps | 1, 5, 10, 20, 50, 100 | Find scheduling saturation |
| k8s_warmpool | target_replicas | 10, 50, 100, 200, 500 | Find provisioning ceiling |
| k8s_deletion | batch_size | 10, 50, 100, 200, 500 | Find cleanup bottleneck |
| k8s_snapshot | preload_mb | 10, 50, 100, 500, 1000 | Find snapshot size limit |

### Execution Order Recommendation

1. **k8s_warmpool** (var1, var3, var4, var8, var10) — fastest to run, no agent API needed
2. **k8s_qps** (var3, var8, var10) — quick runs, tests scheduling
3. **k8s_payload** (var3, var8) — moderate duration
4. **k8s_chromium_density** (var3, var7, var8) — needs chromium warm pool
5. **k8s_deletion** (baseline, var4, var10) — needs bulk provisioning first
6. **k8s_snapshot** (var3, var4, var8) — needs snapshot infrastructure

### Full Test Matrix (Y = Run)

| Variant | k8s_python_density | k8s_warmpool | k8s_qps | k8s_payload | k8s_chromium_density | k8s_deletion | k8s_snapshot |
|---------|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| baseline | Y | Y | Y | Y | Y | Y | Y |
| var1 (kubelet) | Y | Y | | | | | |
| var2 (overlay) | Y | | | | | | |
| var3 (2x CPU) | Y | Y | Y | Y | Y | | Y |
| var4 (disk) | Y | Y | | | | Y | Y |
| var5 (THP) | Y | | | | | | |
| var6 (sched) | Y | | | | | | |
| var7 (swap) | Y | | | | Y | | |
| var8 (AMD) | Y | Y | Y | Y | Y | | Y |
| var9 (ARM) | Y | | | | | | |
| var10 (multi-node) | Y | Y | Y | | | Y | Y |
| combined | Y | Y | | | | | |
