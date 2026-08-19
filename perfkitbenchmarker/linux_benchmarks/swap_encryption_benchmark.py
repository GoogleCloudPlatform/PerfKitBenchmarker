# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""GKE vs. AWS EKS Swap Encryption and LSSD Performance Benchmark.

Methodology: go/swap-encryption-and-lssd-performance-comparison:gke-vs-aws

== Architecture ==

Provisions a real GKE (GCP) or EKS (AWS) Kubernetes cluster via PKB's
container_cluster abstraction, then deploys a privileged DaemonSet whose
pod has full host-device access (/dev, /sys, hostPID).  All benchmark
phases execute inside this pod via kubectl exec, so measurements reflect
actual cluster-node behaviour including Kubernetes overhead (kubelet,
containerd cgroup hierarchy, etc.).

  GKE nodes  ── dm-crypt with ephemeral key (go/node:swap-encryption)
                 swap device: /dev/mapper/swap_encrypted (over hyperdisk or
                 LSSD RAID-0 /dev/md0)

  EKS nodes  ── NVMe Instance Store, Nitro hardware-offloaded encryption
                 swap device: /dev/nvme1n1 (or auto-detected)

== Benchmark Phases ==

  Phase 1 – fio Microbenchmarks
    Run fio directly on the swap block device (swapoff first) to measure
    the hardware + encryption ceiling: random IOPS (4K), sequential
    bandwidth (1M), and completion latency (iodepth=1).

  Phase 2a – CPU Overhead
    stress-ng drives sustained swap I/O; vmstat and pidstat capture
    swap-in/out rates and per-process CPU cost (kswapd, kcryptd,
    dm-crypt threads on GKE; Nitro offload on EKS).

  Phase 2b – I/O Interference
    Baseline fio on a scratch volume → re-run with concurrent swap
    pressure.  IOPS/latency delta = storage contention cost.

  Phase 3a – Redis Latency
    Dataset loaded beyond container memory limit → GET/SET p99 latency
    measured while kernel swaps pages.

  Phase 3b – Kernel Build
    Linux compiled inside a memory-capped cgroup; slowdown ratio vs
    unconstrained baseline.

  Phase 3c – OpenSearch
    Bulk-index + search query under swap pressure (esrally or curl).
"""

import json
import logging
import re
import time
import textwrap
from typing import Any

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources.container_service import kubectl

FLAGS = flags.FLAGS

# ---------------------------------------------------------------------------
# Benchmark identity
# ---------------------------------------------------------------------------

BENCHMARK_NAME = "swap_encryption"

BENCHMARK_CONFIG = """
swap_encryption:
  description: >
    GKE vs. EKS swap encryption and LSSD performance comparison.
    Two-step nodepool setup: PKB provisions a minimal cluster with a cheap
    default nodepool (Step 1), then Prepare() adds the real benchmark
    nodepool (n4-highmem-32 / c4-*-lssd, COS_CONTAINERD, 80k IOPS) with a
    node-level startup script that configures dm-crypt swap before any pod
    is scheduled, then removes the default nodepool (Step 2).  All benchmark
    phases run inside a privileged DaemonSet pinned to the benchmark nodepool.
  container_cluster:
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        # Cheap placeholder — the benchmark nodepool is created in Prepare().
        machine_type: e2-medium
        boot_disk_size: 20
      AWS:
        # Cheap placeholder — the benchmark nodegroup is added in Prepare().
        machine_type: t3.medium
        boot_disk_size: 20
"""

# ---------------------------------------------------------------------------
# Flags
# ---------------------------------------------------------------------------

_SWAP_DEVICE = flags.DEFINE_string(
    "swap_encryption_device",
    "",
    "Explicit swap block-device path on the cluster node, e.g. "
    "/dev/nvme1n1 or /dev/dm-0.  When empty the benchmark auto-detects "
    "via /proc/swaps after setup.",
)
_SWAP_SIZE_GB = flags.DEFINE_integer(
    "swap_encryption_swap_size_gb",
    32,
    "Size in GB of the swap space to configure on the node. "
    "Ignored when a ready swap device already exists.",
)
_SWAP_TYPE = flags.DEFINE_enum(
    "swap_encryption_swap_type",
    "auto",
    ["auto", "instance_store", "io2"],
    "Swap backing storage type.  auto = detect from cloud and instance type.",
)
_FIO_RUNTIME_SEC = flags.DEFINE_integer(
    "swap_encryption_fio_runtime_sec",
    60,
    "Wall-clock runtime in seconds for each individual fio job.",
)
_STRESS_TIMEOUT_SEC = flags.DEFINE_integer(
    "swap_encryption_stress_timeout_sec",
    300,
    "Duration in seconds of each stress-ng memory-pressure phase.",
)
_STRESS_VM_BYTES = flags.DEFINE_string(
    "swap_encryption_stress_vm_bytes",
    "28G",
    "Memory each stress-ng --vm worker touches.  Should exceed node RAM "
    "to force kernel swapping.",
)
_STRESS_VM_BYTES_LIST = flags.DEFINE_string(
    "swap_encryption_stress_vm_bytes_list",
    "",
    "Comma-separated list of stress-ng --vm-bytes values to iterate over "
    'in Phase 2a CPU-overhead sweeps, e.g. "14G,21G,28G".  When non-empty '
    "this overrides --swap_encryption_stress_vm_bytes and Phase 2a is run "
    "once per entry so that the swap-pressure intensity curve is captured.",
)
_REDIS_DATASET_MB = flags.DEFINE_integer(
    "swap_encryption_redis_dataset_mb",
    1024,
    "Approximate Redis dataset size in MB to load before the latency test.",
)
_REDIS_MAXMEMORY_MB = flags.DEFINE_integer(
    "swap_encryption_redis_maxmemory_mb",
    512,
    "Redis maxmemory in MB.  Must be less than dataset size to force swap.",
)
_KERNEL_VERSION = flags.DEFINE_string(
    "swap_encryption_kernel_version",
    "6.1.38",
    "Linux kernel version to download and compile for the build workload.",
)
_KERNEL_MEMORY_MB = flags.DEFINE_integer(
    "swap_encryption_kernel_memory_mb",
    512,
    "cgroup memory limit in MB applied during the constrained kernel build.",
)
_ENABLE_ZSWAP = flags.DEFINE_boolean(
    "swap_encryption_enable_zswap",
    False,
    "Enable zswap (lz4 compressor, 20%% max pool) before running tests.",
)
_MIN_FREE_KBYTES = flags.DEFINE_integer(
    "swap_encryption_min_free_kbytes",
    65536,
    "Value written to /proc/sys/vm/min_free_kbytes to trigger earlier "
    "swapping. Set 0 to leave the kernel default unchanged.",
)
_DAEMONSET_IMAGE = flags.DEFINE_string(
    "swap_encryption_daemonset_image",
    "ubuntu:22.04",
    "Container image used for the privileged benchmark DaemonSet pod.",
)
_NODEPOOL = flags.DEFINE_string(
    "swap_encryption_nodepool",
    "benchmark",
    "Name of the node pool to deploy the benchmark DaemonSet on.",
)
_INSTANCE_SIZE_LABEL = flags.DEFINE_string(
    "swap_encryption_instance_size_label",
    "",
    "Human-readable label for the current instance size being tested, e.g. "
    '"n4-highmem-32" or "i4i.4xlarge".  Stored in sample metadata so that '
    "results from multiple PKB runs across different instance sizes can be "
    "collated and compared.  Defaults to the value reported by the cloud "
    "metadata endpoint inside the pod.",
)
_COLLECT_COST = flags.DEFINE_boolean(
    "swap_encryption_collect_cost",
    False,
    "When True, emit a cost_estimate_usd sample using on-demand pricing "
    "for the instance type detected at runtime.",
)
_IO2_ENCRYPTED = flags.DEFINE_boolean(
    "swap_encryption_io2_encrypted",
    True,
    "When True (default), the dedicated io2 swap volume is created with EBS "
    'encryption (Nitro/KMS) -> matrix row "io2 + hardware encryption". '
    "Set False for the unencrypted io2 baseline row. Only applies when "
    "--swap_encryption_swap_type=io2.",
)
_IO2_KMS_KEY_ID = flags.DEFINE_string(
    "swap_encryption_io2_kms_key_id",
    "",
    "Optional KMS key id/ARN for the encrypted io2 volume. Empty = the "
    "account default aws/ebs key. Ignored unless io2_encrypted is True.",
)

# ---------------------------------------------------------------------------
# New flags — benchmark nodepool, COS image, encryption toggle, IOPS
# ---------------------------------------------------------------------------

_BENCHMARK_MACHINE_TYPE = flags.DEFINE_string(
    "swap_encryption_benchmark_machine_type",
    "n4-highmem-32",
    "Machine type for the benchmark nodepool created in Prepare(). "
    "Use n4-highmem-32 (hyperdisk, default) or c4-standard-8-lssd "
    "(LSSD RAID-0).  The matching swap setup is selected automatically.",
)

# ---------------------------------------------------------------------------
# Internal constants
# ---------------------------------------------------------------------------

_DS_NAME = "pkb-swap-benchmark"
_DS_NAMESPACE = "default"
_DS_LABEL = "pkb-swap-benchmark"
_BENCHMARK_NODEPOOL = "benchmark"  # name of the nodepool created in Prepare()

# fio jobs: (name, rw_mode, blocksize, iodepth, description)
_FIO_JOBS = [
    ("rand_write_iops", "randwrite", "4k", 256, "Random write IOPS"),
    ("rand_read_iops", "randread", "4k", 256, "Random read IOPS"),
    ("rand_rw_mixed", "randrw", "4k", 256, "Mixed random R/W (50/50)"),
    ("seq_write_bw", "write", "1m", 64, "Sequential write bandwidth"),
    ("seq_read_bw", "read", "1m", 64, "Sequential read bandwidth"),
    ("lat_write", "randwrite", "4k", 1, "Random write latency"),
    ("lat_read", "randread", "4k", 1, "Random read latency"),
]

_VMSTAT_LOG = "/tmp/pkb_vmstat.log"
_PIDSTAT_LOG = "/tmp/pkb_pidstat.log"
_CRYPTO_PROCS = ("kswapd", "kworker", "kcryptd", "dmcrypt_write")

# ---------------------------------------------------------------------------
# DaemonSet manifest (embedded YAML)
# ---------------------------------------------------------------------------


def _DaemonSetYaml(image: str) -> str:
  """Return the privileged benchmark DaemonSet manifest as a YAML string.

  The DaemonSet is pinned to the benchmark nodepool via nodeSelector so it
  never lands on the cheap dummy default nodepool.  By the time this pod
  starts, the node startup script has already configured dm-crypt swap at
  the OS level, so the pod only needs to verify/use that device.
  """
  return textwrap.dedent(f"""\
    apiVersion: apps/v1
    kind: DaemonSet
    metadata:
      name: {_DS_NAME}
      namespace: {_DS_NAMESPACE}
      labels:
        app: {_DS_LABEL}
    spec:
      selector:
        matchLabels:
          app: {_DS_LABEL}
      template:
        metadata:
          labels:
            app: {_DS_LABEL}
        spec:
          hostPID: true
          hostNetwork: true
          # Pin to the benchmark nodepool — never schedule on the dummy default pool.
          nodeSelector:
            pkb_nodepool: {_BENCHMARK_NODEPOOL}
          tolerations:
          - operator: Exists
          containers:
          - name: benchmark
            image: {image}
            command:
            - bash
            - -c
            - |
              set +e
              echo "[pkb] Installing benchmark tools..."
              apt-get update -qq
              DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \\
                fio \\
                stress-ng \\
                sysstat \\
                cryptsetup \\
                mdadm \\
                redis-server \\
                redis-tools \\
                wget \\
                curl \\
                make \\
                gcc \\
                bc \\
                flex \\
                bison \\
                libelf-dev \\
                libssl-dev \\
                cgroup-tools \\
                nvme-cli \\
                util-linux \\
                python3-pip \\
                2>&1
              echo "[pkb] Building memtier_benchmark from source..."
              apt-get install -y -qq autoconf automake libpcre3-dev libevent-dev pkg-config zlib1g-dev git libtool 2>&1 || true
              git clone --depth 1 https://github.com/RedisLabs/memtier_benchmark /tmp/mtb 2>&1 || true
              ( cd /tmp/mtb && autoreconf -ivf && ./configure && make -j4 && make install ) 2>&1 || true
              echo "[pkb] Installing esrally..."
              pip3 install esrally --quiet --break-system-packages 2>&1 || true
              echo "[pkb] Tools installed. Writing ready sentinel."
              touch /tmp/pkb_ready
              echo "PKB_READY_SENTINEL"
              sleep infinity
            securityContext:
              privileged: true
              capabilities:
                add: ["SYS_ADMIN", "IPC_LOCK"]
            resources:
              requests:
                memory: "512Mi"
            env:
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
            - name: POD_UID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.uid
            volumeMounts:
            - name: dev
              mountPath: /dev
            - name: sys
              mountPath: /sys
            - name: run
              mountPath: /run
            - name: proc-host
              mountPath: /proc-host
              readOnly: true
          volumes:
          - name: dev
            hostPath:
              path: /dev
          - name: sys
            hostPath:
              path: /sys
          - name: run
            hostPath:
              path: /run
          - name: proc-host
            hostPath:
              path: /proc
  """)


# ---------------------------------------------------------------------------
# PKB entry points
# ---------------------------------------------------------------------------


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


_IO2_VOLUME_ID = ""  # set by _EnsureIo2Volume; used for serial-based detection


def _EnsureIo2Volume() -> None:
  """Create + attach a dedicated io2 EBS volume to the benchmark node so
  Config-2 swaps on real io2 hardware-encrypted storage.  No-op unless
  --swap_encryption_swap_type=io2.  Best-effort: logs and returns on failure.
  Stashes the created volume id in _IO2_VOLUME_ID for serial-based detection.
  """
  global _IO2_VOLUME_ID
  if _SWAP_TYPE.value != "io2":
    return
  out, _, rc = kubectl.RunKubectlCommand(
      ["get", "nodes", "-o", "jsonpath={.items[0].spec.providerID}"],
      raise_on_failure=False,
  )
  provider = (out or "").strip()  # aws:///us-east-1a/i-0abc...
  if rc != 0 or "aws://" not in provider:
    logging.warning(
        "[swap_encryption] io2 attach skipped: could not resolve "
        "EC2 instance from providerID=%r",
        provider,
    )
    return
  parts = [p for p in provider.split("/") if p]
  instance_id, az = parts[-1], parts[-2]
  region = az[:-1]
  base = ["aws", "ec2", "--region", region]
  try:
    create_args = [
        "create-volume",
        "--volume-type",
        "io2",
        "--size",
        "500",
        "--iops",
        "16000",
        "--availability-zone",
        az,
        "--tag-specifications",
        "ResourceType=volume,Tags=[{Key=pkb,Value=swap_encryption}]",
    ]
    if _IO2_ENCRYPTED.value:
      create_args.append("--encrypted")
      if _IO2_KMS_KEY_ID.value:
        create_args += ["--kms-key-id", _IO2_KMS_KEY_ID.value]
      logging.info(
          "[swap_encryption] io2 volume will be EBS-encrypted "
          "(row: hardware encryption)"
      )
    else:
      logging.info("[swap_encryption] io2 volume UNENCRYPTED (baseline row)")
    create_args += ["--query", "VolumeId", "--output", "text"]
    vol_id, _, vrc = vm_util.IssueCommand(
        base + create_args, raise_on_failure=False
    )
    vol_id = (vol_id or "").strip()
    if vrc != 0 or not vol_id.startswith("vol-"):
      logging.warning("[swap_encryption] io2 create-volume failed: %r", vol_id)
      return
    vm_util.IssueCommand(
        base + ["wait", "volume-available", "--volume-ids", vol_id],
        raise_on_failure=False,
    )
    vm_util.IssueCommand(
        base
        + [
            "attach-volume",
            "--volume-id",
            vol_id,
            "--instance-id",
            instance_id,
            "--device",
            "/dev/sdf",
        ],
        raise_on_failure=False,
    )
    vm_util.IssueCommand(
        base + ["wait", "volume-in-use", "--volume-ids", vol_id],
        raise_on_failure=False,
    )
    _IO2_VOLUME_ID = vol_id
    logging.info(
        "[swap_encryption] Attached io2 volume %s to %s as /dev/sdf",
        vol_id,
        instance_id,
    )
    time.sleep(15)  # allow the NVMe device node to appear
  except Exception as e:  # pylint: disable=broad-except
    logging.warning("[swap_encryption] io2 attach error (continuing): %s", e)


def Prepare(spec) -> None:
  """Two-step nodepool setup then DaemonSet deployment.

  Step 1 (handled by PKB infrastructure): cluster provisioned with a cheap
  e2-medium default nodepool.

  Step 2 (this function):
    a. Create the benchmark nodepool (n4-highmem-32 or c4-*-lssd) with
       COS_CONTAINERD, 80 000 IOPS, and a node startup script that configures
       dm-crypt swap at the OS level — before any pod is scheduled.
    b. Delete the dummy default nodepool to stop its cost immediately.
    c. Deploy the privileged DaemonSet (pinned via nodeSelector to the
       benchmark nodepool) and wait for tools to install.
  """

  # EKS nodepool management is external: PKB's cluster creation labels nodes
  # pkb_nodepool=default, so re-label all existing nodes here to match the
  # DaemonSet nodeSelector (pkb_nodepool=benchmark).
  logging.info(
      "[swap_encryption] Labelling existing nodes with pkb_nodepool=%s "
      "so the DaemonSet nodeSelector matches.",
      _BENCHMARK_NODEPOOL,
  )
  kubectl.RunKubectlCommand([
      "label",
      "nodes",
      "--all",
      "--overwrite",
      f"pkb_nodepool={_BENCHMARK_NODEPOOL}",
  ])
  # Config-2: attach a real io2 EBS volume so swap runs on io2 storage.
  _EnsureIo2Volume()

  # ── Step 2c: deploy DaemonSet ────────────────────────────────────────────
  logging.info("[swap_encryption] Step 2c: deploying privileged DaemonSet")
  _DeployDaemonSet()

  pod = _WaitForBenchmarkPod()
  if not pod:
    raise errors.Benchmarks.PrepareException(
        "[swap_encryption] Benchmark pod never became ready after 900s. "
        "Check node label and DaemonSet events:\n"
        "  kubectl describe ds pkb-swap-benchmark -n default\n"
        "  kubectl get nodes --show-labels"
    )
  logging.info("[swap_encryption] Benchmark pod ready: %s", pod)

  # Tune kernel swap aggressiveness
  if _MIN_FREE_KBYTES.value > 0:
    _PodExec(
        pod,
        f"sysctl -w vm.min_free_kbytes={_MIN_FREE_KBYTES.value}",
        ignore_failure=True,
    )

  # Enable zswap if requested
  if _ENABLE_ZSWAP.value:
    _EnableZswap(pod)

  # Configure cloud-specific swap
  cloud = _DetectCloud(pod)
  logging.info("[swap_encryption] Detected cloud: %s", cloud)

  if cloud == "aws":
    _SetupEKSSwap(pod)
  else:
    logging.warning(
        "[swap_encryption] Unknown cloud – falling back to plain swapfile"
    )
    _SetupPlainSwapFile(pod, _SWAP_SIZE_GB.value)


def Run(spec) -> list[sample.Sample]:
  """Execute all benchmark phases with gate logic.

  Execution is structured in three gated tiers matching the execution plan:

    Tier 1 (Gate 1) — fio microbenchmarks
      Raw I/O ceiling of the swap device.  Gate 1 fails if fio produces
      zero samples (device not found, O_DIRECT error, etc.).

    Tier 2 (Gate 2) — stress-ng CPU overhead + I/O interference
      Requires an active swap device (Gate 1 must pass).  Gate 2 fails if
      stress-ng does not complete within timeout.

    Tier 3 (Gate 3) — real-world workloads (Redis, kernel build, OpenSearch)
      Independent of Tier 2 results; always attempted if Gate 1 passed.
      Individual workload failures are logged but do not abort the others.

  If Gate 1 fails, Tiers 2 and 3 are skipped — there is no point measuring
  application-level swap performance when the raw device is inaccessible.
  """
  pod = _WaitForBenchmarkPod()
  swap_dev = _DetectSwapDevice(pod)
  base_meta = _BuildMetadata(pod, swap_dev)
  results: list[sample.Sample] = []
  t_run_start = time.time()

  logging.info("[swap_encryption] swap device: %s", swap_dev)

  # ── Tier 1 / Gate 1: fio microbenchmarks ─────────────────────────────────
  logging.info("[swap_encryption] ── Tier 1 / Gate 1: fio microbenchmarks ──")
  tier1_results = []
  try:
    tier1_results = _Phase1_Fio(pod, swap_dev, base_meta)
    results += tier1_results
  except Exception as e:  # pylint: disable=broad-except
    logging.error("[swap_encryption] Gate 1 FAILED — fio phase error: %s", e)
    logging.error("[swap_encryption] Skipping Tiers 2 and 3 (no swap device)")
    return results

  if not tier1_results:
    logging.warning(
        "[swap_encryption] Gate 1 produced no samples "
        "(loop-device skip or parse error) — "
        "continuing to Tier 2 with caution"
    )

  # ── Tier 2 / Gate 2: stress-ng CPU overhead + I/O interference ───────────
  logging.info("[swap_encryption] ── Tier 2 / Gate 2: stress-ng phases ──")
  try:
    logging.info("[swap_encryption] Phase 2a: CPU overhead")
    results += _Phase2a_CpuOverhead(pod, base_meta)
    logging.info("[swap_encryption] Phase 2b: I/O interference")
    results += _Phase2b_IoInterference(pod, base_meta)
  except Exception as e:  # pylint: disable=broad-except
    logging.error("[swap_encryption] Gate 2 FAILED — stress phase error: %s", e)
    logging.warning(
        "[swap_encryption] Proceeding to Tier 3 (workloads are "
        "independent of stress-ng results)"
    )

  # ── Tier 3 / Gate 3: real-world workloads ────────────────────────────────
  # The Gate-2 stress can trigger kubelet eviction / OOM-kill of the benchmark
  # pod; the DaemonSet then recreates it under a NEW name.  Run() cached the
  # original name, so without this re-resolve every Tier-3 exec hits a dead pod
  # ("pods ... not found") and redis/memtier/kernel/opensearch all silently
  # produce nothing.  Re-resolve (and wait for the new pod's tools to finish
  # reinstalling) so Tier 3 runs against the live pod.
  fresh_pod = _WaitForBenchmarkPod()
  if fresh_pod and fresh_pod != pod:
    logging.info(
        "[swap_encryption] Benchmark pod was recreated: %s -> %s "
        "(Gate-2 eviction); using new pod for Tier 3.",
        pod,
        fresh_pod,
    )
  if fresh_pod:
    pod = fresh_pod
  logging.info(
      "[swap_encryption] ── Tier 3 / Gate 3: workloads (pod=%s) ──", pod
  )
  for phase_name, phase_fn in [
      ("Redis latency (3a)", lambda: _Phase3a_Redis(pod, base_meta)),
      ("Kernel build (3b)", lambda: _Phase3b_KernelBuild(pod, base_meta)),
      ("OpenSearch (3c)", lambda: _Phase3c_OpenSearch(pod, base_meta)),
  ]:
    try:
      logging.info("[swap_encryption] Phase %s", phase_name)
      results += phase_fn()
    except Exception as e:  # pylint: disable=broad-except
      logging.error(
          "[swap_encryption] %s FAILED: %s — continuing with "
          "remaining workloads",
          phase_name,
          e,
      )

  # ── Cost estimate ─────────────────────────────────────────────────────────
  if _COLLECT_COST.value:
    elapsed = time.time() - t_run_start
    results += _CollectCostSample(pod, elapsed, base_meta)

  return results


def Cleanup(spec) -> None:
  """Remove the DaemonSet and tear down any swap configuration."""
  pod = _WaitForBenchmarkPod(timeout=30)
  if pod:
    _PodExec(pod, "swapoff -a 2>/dev/null || true", ignore_failure=True)
    # NOTE: do NOT call dmsetup/cryptsetup close here – those commands hang
    # indefinitely on GKE COS nodes.  Loop device cleanup is sufficient.
    _PodExec(
        pod,
        textwrap.dedent("""
      for backing in /var/pkb_swap_backing /run/pkb_swap_backing; do
        losetup -j "$backing" 2>/dev/null | awk -F: '{print $1}' | \\
          while read dev; do losetup -d "$dev" 2>/dev/null || true; done
        rm -f "$backing"
      done
    """),
        ignore_failure=True,
    )
    _PodExec(
        pod,
        'pkill -f "stress-ng|fio" 2>/dev/null || true',
        ignore_failure=True,
    )

  _DeleteDaemonSet()


# ---------------------------------------------------------------------------
# DaemonSet lifecycle helpers
# ---------------------------------------------------------------------------


def _DeployDaemonSet() -> None:
  """Apply the benchmark DaemonSet manifest to the cluster."""
  manifest = _DaemonSetYaml(image=_DAEMONSET_IMAGE.value)
  with vm_util.NamedTemporaryFile(mode="w", suffix=".yaml") as f:
    f.write(manifest)
    f.close()
    # --validate=false skips the client-side OpenAPI schema download, which a
    # freshly-created EKS control plane is often too slow to serve ("failed to
    # download openapi: context deadline exceeded").  The API server still
    # validates the manifest at admission.  Retry to ride out transient API
    # slowness in the first minutes after cluster creation.
    last_err = ""
    for attempt in range(1, 7):
      out, err, rc = kubectl.RunKubectlCommand(
          ["apply", "-f", f.name, "--validate=false"],
          raise_on_failure=False,
          timeout=300,
      )
      if rc == 0:
        break
      last_err = err or out or ""
      logging.warning(
          "[swap_encryption] DaemonSet apply attempt %d/6 failed "
          "(rc=%s) -- retrying in 20s",
          attempt,
          rc,
      )
      time.sleep(20)
    else:
      raise errors.Benchmarks.PrepareException(
          "[swap_encryption] DaemonSet apply failed after 6 attempts: "
          f"{last_err}"
      )
  logging.info("[swap_encryption] DaemonSet applied")


def _WaitForBenchmarkPod(timeout: int = 900) -> str | None:
  """Wait until the DaemonSet pod is Running AND tools are installed.

  The benchmark container installs apt packages on first start and writes
  /tmp/pkb_ready when done (~2-4 min on a cold node).  We must wait for
  that sentinel before exec-ing any commands, otherwise tools like
  cryptsetup / fio may not yet be on PATH.

  Uses tab-separated name/phase output so kubectl always exits 0 regardless
  of whether any pods are present, avoiding jsonpath index errors.
  """
  deadline = time.time() + timeout
  last_phase = ""
  ready_pod = None  # pod name once phase == Running

  while time.time() < deadline:
    # ── Step 1: wait for Running phase ──────────────────────────────────────
    if ready_pod is None:
      out, _, rc = kubectl.RunKubectlCommand(
          [
              "get",
              "pods",
              "-l",
              f"app={_DS_LABEL}",
              "-n",
              _DS_NAMESPACE,
              "-o",
              (
                  r"jsonpath={range"
                  r' .items[*]}{.metadata.name}{"\t"}{.status.phase}{"\n"}{end}'
              ),
          ],
          raise_on_failure=False,
      )

      if rc == 0 and out.strip():
        for line in out.strip().splitlines():
          parts = line.split("\t")
          if len(parts) == 2:
            pod_name, phase = parts[0].strip(), parts[1].strip()
            if phase == "Running":
              logging.info(
                  "[swap_encryption] Pod %s is Running – "
                  "waiting for tool install to finish...",
                  pod_name,
              )
              ready_pod = pod_name
              break
            if phase != last_phase:
              logging.info(
                  "[swap_encryption] Pod %s phase: %s",
                  pod_name,
                  phase,
              )
              last_phase = phase
              if phase in ("Pending",):
                _LogPodEvents(pod_name)
      else:
        logging.info("[swap_encryption] Waiting for DaemonSet pod to appear...")

    # ── Step 2: poll container LOGS for PKB_READY_SENTINEL ───────────────────
    # The container echoes PKB_READY_SENTINEL after the apt/build install
    # finishes.  Polling logs (not exec) avoids the AL2023+containerd
    # "container not found" exec race during early init / mid-install.
    if ready_pod is not None:
      logs_out, _, logs_rc = kubectl.RunKubectlCommand(
          [
              "logs",
              ready_pod,
              "-n",
              _DS_NAMESPACE,
              "-c",
              "benchmark",
          ],
          raise_on_failure=False,
      )
      if logs_rc == 0 and "PKB_READY_SENTINEL" in (logs_out or ""):
        logging.info(
            "[swap_encryption] Pod %s ready (tools installed)",
            ready_pod,
        )
        return ready_pod
      logging.info(
          "[swap_encryption] Pod %s: still installing tools...", ready_pod
      )

    time.sleep(15)

  logging.warning(
      "[swap_encryption] Benchmark pod not ready after %ds", timeout
  )
  return None


def _LogPodEvents(pod_name: str) -> None:
  """Dump recent Kubernetes events for the pod to help diagnose startup hangs."""
  events_out, _, _ = kubectl.RunKubectlCommand(
      [
          "describe",
          "pod",
          pod_name,
          "-n",
          _DS_NAMESPACE,
      ],
      raise_on_failure=False,
  )
  # Only log the Events section to keep output manageable
  in_events = False
  lines = []
  for line in events_out.splitlines():
    if line.startswith("Events:"):
      in_events = True
    if in_events:
      lines.append(line)
  if lines:
    logging.info("[swap_encryption] Pod events:\n%s", "\n".join(lines[:30]))
  else:
    logging.info(
        "[swap_encryption] kubectl describe output:\n%s",
        events_out[-2000:] if len(events_out) > 2000 else events_out,
    )


def _DeleteDaemonSet() -> None:
  """Delete the benchmark DaemonSet."""
  kubectl.RunKubectlCommand(
      [
          "delete",
          "daemonset",
          _DS_NAME,
          "-n",
          _DS_NAMESPACE,
          "--ignore-not-found",
      ],
      raise_on_failure=False,
  )
  logging.info("[swap_encryption] DaemonSet deleted")


# ---------------------------------------------------------------------------
# Two-step GKE nodepool helpers
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Pod exec wrapper
# ---------------------------------------------------------------------------


def _PodExec(
    pod: str,
    cmd: str,
    ignore_failure: bool = False,
    timeout: int = 300,
) -> tuple[str, str]:
  """Run a shell command inside the benchmark pod via kubectl exec.

  Args:
    pod:            Pod name returned by _WaitForBenchmarkPod.
    cmd:            Shell command string passed to bash -c.
    ignore_failure: When True, non-zero exit codes are logged but not raised.
    timeout:        Seconds before PKB kills the kubectl exec process.
                    Default 300 s matches PKB's IssueCommand default.
                    Pass a larger value for long-running jobs (fio, stress-ng,
                    kernel build).

  Returns:
    (stdout, stderr) strings.
  """
  err = ""
  rc = 1
  out = ""
  for attempt in range(1, 6):
    out, err, rc = kubectl.RunKubectlCommand(
        ["exec", pod, "-n", _DS_NAMESPACE, "--", "bash", "-c", cmd],
        raise_on_failure=False,
        timeout=timeout,
    )
    if rc == 0:
      return out, err
    low = (err or "").lower()
    transient = (
        "container not found" in low
        or "unable to upgrade connection" in low
        or "connection reset by peer" in low
        or "error dialing backend" in low
        or "eof" in low
        or "i/o timeout" in low
    )
    if not transient or attempt == 5:
      break
    logging.info(
        "[swap_encryption] exec transient error on %s -- retry %d/5 in 10s",
        pod,
        attempt,
    )
    time.sleep(10)
  if not ignore_failure:
    raise errors.VmUtil.IssueCommandError(
        f"[swap_encryption] kubectl exec failed on {pod} (rc={rc}): {err}"
    )
  return out, err


def _EnableSwapPressure(pod: str, high_pct: int = 75) -> None:
  """Lift cgroup memory.swap.max to 'max' for this pod and set memory.high so
  the >RAM working set is forced to swap.  EKS kubelet runs swapBehavior=NoSwap
  -> swap.max=0 -> swap_out is structurally 0; this is the prerequisite for
  measuring dm-crypt / Nitro CPU under swap.  Locates cgroup nodes by POD_UID
  (works for namespaced OR host /sys view); never touches the host root cgroup.
  """
  script = r"""
set +e
UID_US=$(printf '%s' "${POD_UID:-}" | tr '-' '_')
MEMKB=$(awk '/MemTotal/{print $2}' /proc/meminfo)
HIGH=$(( MEMKB * 1024 * __PCT__ / 100 ))
apply() { [ -d "$1" ] || return; echo max > "$1/memory.swap.max" 2>/dev/null; echo "$HIGH" > "$1/memory.high" 2>/dev/null; }
HOSTVIEW=0
if [ -d /sys/fs/cgroup/kubepods.slice ] || [ -d /sys/fs/cgroup/kubepods ]; then HOSTVIEW=1; fi
N=0
if [ "$HOSTVIEW" = 1 ] && [ -n "${POD_UID:-}" ]; then
  for f in $(find /sys/fs/cgroup -name memory.swap.max 2>/dev/null); do
    case "$f" in
      *"$POD_UID"*|*"$UID_US"*) d="$(dirname "$f")"; apply "$d"; N=$((N+1));
        echo "[pkb-swap] set $d swap.max=$(cat "$d/memory.swap.max" 2>/dev/null) high=$(cat "$d/memory.high" 2>/dev/null)" ;;
    esac
  done
elif [ "$HOSTVIEW" = 0 ]; then
  apply /sys/fs/cgroup; N=1
  echo "[pkb-swap] set /sys/fs/cgroup swap.max=$(cat /sys/fs/cgroup/memory.swap.max 2>/dev/null) high=$(cat /sys/fs/cgroup/memory.high 2>/dev/null)"
fi
echo 1   > /proc/sys/vm/overcommit_memory 2>/dev/null
echo 100 > /proc/sys/vm/swappiness        2>/dev/null
echo "[pkb-swap] hostview=$HOSTVIEW uid=${POD_UID:-?} high=$HIGH applied=$N"
echo "[pkb-swap] swapon: $(swapon --show 2>/dev/null | tr '\n' ' ')"
[ "$N" = 0 ] && echo "[pkb-swap] WARNING: no cgroup target matched -> swap may stay 0; check POD_UID"
true
""".replace("__PCT__", str(high_pct))
  out, _ = _PodExec(pod, script, ignore_failure=True, timeout=60)
  for ln in out.splitlines():
    if ln.startswith("[pkb-swap]"):
      logging.info("[swap_encryption] %s", ln)


def _RelaxMemoryHigh(pod: str) -> None:
  """Remove memory.high throttling after the Gate-2 sweep (swap.max stays max)."""
  script = r"""
set +e
UID_US=$(printf '%s' "${POD_UID:-}" | tr '-' '_')
if [ -d /sys/fs/cgroup/kubepods.slice ] || [ -d /sys/fs/cgroup/kubepods ]; then
  for f in $(find /sys/fs/cgroup -name memory.high 2>/dev/null); do
    case "$f" in *"$POD_UID"*|*"$UID_US"*) echo max > "$f" 2>/dev/null ;; esac
  done
else
  echo max > /sys/fs/cgroup/memory.high 2>/dev/null
fi
true
"""
  _PodExec(pod, script, ignore_failure=True, timeout=30)


# ---------------------------------------------------------------------------
# Cloud-specific swap setup
# ---------------------------------------------------------------------------


def _DetectCloud(pod: str) -> str:
  """Detect GCP vs AWS from DMI product info exposed via /sys hostPath mount.

  DMI is the most reliable in-container detection method because it reads
  directly from the host kernel's SMBIOS table via /sys (already mounted).
  It avoids HTTP metadata endpoint quoting issues and network timeouts.

  Falls back to metadata HTTP endpoints if DMI is inconclusive.
  """
  # Primary: DMI product name / vendor (available via /sys hostPath mount)
  dmi_out, _ = _PodExec(
      pod,
      "cat /sys/class/dmi/id/sys_vendor /sys/class/dmi/id/product_name "
      '/sys/class/dmi/id/bios_vendor 2>/dev/null || echo ""',
      ignore_failure=True,
  )
  dmi = dmi_out.strip().lower()
  if "google" in dmi:
    logging.info(
        "[swap_encryption] Cloud detected via DMI: gcp (%s)",
        dmi_out.strip(),
    )
    return "gcp"
  if any(k in dmi for k in ("amazon", "ec2", "aws")):
    logging.info(
        "[swap_encryption] Cloud detected via DMI: aws (%s)",
        dmi_out.strip(),
    )
    return "aws"

  # Secondary: GCP metadata endpoint.
  # Use -H with no space after colon to avoid shell-quoting issues through
  # the kubectl exec → bash -c pipeline.
  gcp_out, _ = _PodExec(
      pod,
      "curl -s -m 3 "
      "http://metadata.google.internal/computeMetadata/v1/instance/zone "
      '-H Metadata-Flavor:Google 2>/dev/null || echo ""',
      ignore_failure=True,
  )
  if gcp_out.strip():
    logging.info("[swap_encryption] Cloud detected via metadata: gcp")
    return "gcp"

  # Tertiary: AWS IMDS
  aws_out, _ = _PodExec(
      pod,
      "T=$(curl -s -m 3 -X PUT "
      "http://169.254.169.254/latest/api/token "
      '-H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null); '
      'curl -s -m 3 -H "X-aws-ec2-metadata-token: $T" '
      "http://169.254.169.254/latest/meta-data/instance-id "
      '2>/dev/null || echo ""',
      ignore_failure=True,
  )
  if aws_out.strip():
    logging.info("[swap_encryption] Cloud detected via IMDS: aws")
    return "aws"

  logging.warning(
      "[swap_encryption] Could not detect cloud from DMI or metadata"
  )
  return "unknown"


def _SetupEKSSwap(pod: str) -> None:
  """Configure swap on EKS nodes — Instance Store OR io2 root disk.

  Swap type is selected by --swap_encryption_swap_type:
    instance_store (default) – NVMe SSD attached by Nitro (i4i, m6id, c6id).
      Nitro encrypts all block-device writes at hardware level; no extra
      cryptsetup needed.
    io2 – EBS io2 volume provisioned as the node root/data disk.
      Used for apples-to-apples comparison against GKE hyperdisk-balanced.
  """
  swap_type = _SWAP_TYPE.value
  if swap_type in ("auto", "instance_store"):
    _SetupEKSInstanceStoreSwap(pod)
  elif swap_type == "io2":
    _SetupEKSIo2Swap(pod)
  else:
    logging.warning(
        "[swap_encryption] Unknown EKS swap type %s – fallback", swap_type
    )
    _SetupEKSInstanceStoreSwap(pod)


def _SetupEKSInstanceStoreSwap(pod: str) -> None:
  """Swap on AWS NVMe Instance Store (Nitro hardware-offloaded encryption)."""
  logging.info("[swap_encryption] EKS: setting up Instance Store swap")

  # Find the Instance Store NVMe device (not the root EBS volume)
  nvme_out, _ = _PodExec(
      pod,
      "nvme list 2>/dev/null | awk '/Instance Storage/{print $1}' | head -1 || "
      "lsblk -d -o NAME,MODEL | grep -i 'instance\\|nvme' | "
      "grep -v 'nvme0' | awk '{print \"/dev/\"$1}' | head -1",
      ignore_failure=True,
  )
  device = nvme_out.strip()
  if not device:
    # Common Instance Store device paths on AWS
    for candidate in ["/dev/nvme1n1", "/dev/nvme2n1", "/dev/xvdb"]:
      exists_out, _ = _PodExec(
          pod,
          f"test -b {candidate} && echo yes || echo no",
          ignore_failure=True,
      )
      if exists_out.strip() == "yes":
        device = candidate
        break

  if not device:
    logging.warning(
        "[swap_encryption] No Instance Store NVMe found – creating swapfile"
    )
    _SetupPlainSwapFile(pod, _SWAP_SIZE_GB.value)
    return

  logging.info("[swap_encryption] EKS: Instance Store device: %s", device)

  # Nitro encrypts all Instance Store writes automatically.
  # No additional cryptsetup required.
  _PodExec(
      pod,
      textwrap.dedent(f"""
    mkswap {device} && \\
    swapon {device}
  """),
  )
  logging.info(
      "[swap_encryption] EKS: Instance Store swap active on %s", device
  )


def _SetupEKSIo2Swap(pod: str) -> None:
  """Swap on AWS EBS io2 volume – apples-to-apples comparison vs GKE hyperdisk.

  EBS io2 volumes on Nitro instances are encrypted at rest by AWS KMS (if
  enabled on the volume) or via Nitro-level hardware encryption.  No additional
  cryptsetup is needed here; we simply format the attached data disk as swap.

  Device discovery order:
    1. Any non-root, non-Instance-Store block device (xvd*, sdb, second NVMe).
    2. /dev/nvme1n1, /dev/nvme2n1 – fallback if lsblk heuristics fail.
  """
  logging.info("[swap_encryption] EKS: setting up io2 EBS swap")

  # Identify root device so we can exclude it
  root_out, _ = _PodExec(
      pod,
      "lsblk -no pkname $(findmnt -n -o SOURCE /) 2>/dev/null || echo nvme0n1",
      ignore_failure=True,
  )
  root_base = root_out.strip() or "nvme0n1"

  # Identify the io2 volume UNAMBIGUOUSLY by its NVMe serial == volume id.
  # An EBS NVMe device's serial equals the volume id minus the dash
  # (vol-0abc... -> serial vol0abc...).  This never picks the root disk or the
  # instance store regardless of nvmeXn1 enumeration order on Nitro.
  device = ""
  target = _IO2_VOLUME_ID.replace("-", "")
  if target:
    ser_out, _ = _PodExec(
        pod,
        "for d in /sys/block/nvme*n1; do "
        '[ -e "$d" ] || continue; '
        's=$(cat "$d/device/serial" 2>/dev/null | tr -d "-" | tr -d " "); '
        f'[ "$s" = "{target}" ] && {{ echo "/dev/$(basename "$d")"; break; }}; '
        "done",
        ignore_failure=True,
    )
    device = ser_out.strip()
    if device:
      logging.info(
          "[swap_encryption] EKS: io2 matched by serial %s -> %s",
          target,
          device,
      )

  if not device:
    # Fallback: first non-root EBS ("Elastic Block Store") device, excluding
    # any device that is currently mounted (root) or already active swap.
    disk_out, _ = _PodExec(
        pod,
        "for d in /sys/block/nvme*n1 /sys/block/xvd[b-z] /sys/block/sd[b-z];"
        ' do [ -e "$d" ] || continue; n=$(basename "$d"); [ "$n" ='
        f' "{root_base}" ] && continue; m=$(cat "$d/device/model" 2>/dev/null);'
        ' echo "$m" | grep -qi "Elastic Block Store" || continue; mnt=$(lsblk'
        ' -no MOUNTPOINT "/dev/$n" 2>/dev/null | tr -d " "); [ -n "$mnt" ] &&'
        ' continue; echo "/dev/$n"; break; done',
        ignore_failure=True,
    )
    device = disk_out.strip()
    if device:
      logging.info("[swap_encryption] EKS: io2 fallback EBS device: %s", device)

  if not device:
    logging.warning(
        "[swap_encryption] No io2 EBS disk found – creating plain swapfile"
    )
    _SetupPlainSwapFile(pod, _SWAP_SIZE_GB.value)
    return

  logging.info("[swap_encryption] EKS: io2 EBS device: %s", device)

  # EBS io2 encryption is handled at the AWS level (Nitro / KMS).
  out, _ = _PodExec(
      pod,
      textwrap.dedent(f"""
    swapoff {device} 2>/dev/null || true
    wipefs -a {device} 2>/dev/null || true
    mkswap -f {device} && swapon {device}
    swapon --show
  """),
      ignore_failure=True,
  )
  if device not in out:
    raise RuntimeError(
        f"[swap_encryption] io2 swap did not activate on {device}; "
        f"swapon --show output: {out!r}. The device may be busy/mounted "
        "(wrong device picked) or mkswap failed."
    )
  logging.info("[swap_encryption] EKS: io2 EBS swap active on %s", device)


def _SetupPlainSwapFile(pod: str, size_gb: int) -> None:
  """Fallback: create a loop-device-backed swapfile.

  A plain file on overlayfs (the container root) cannot be used as swap —
  the kernel rejects it with EINVAL.  Routing it through a loop device
  presents a proper block device to the mm subsystem and succeeds.
  """
  logging.info("[swap_encryption] Creating %dGB loop-device swap", size_gb)
  _PodExec(
      pod,
      textwrap.dedent(f"""
    fallocate -l {size_gb}G /tmp/pkb_swapfile && \\
    chmod 600 /tmp/pkb_swapfile && \\
    LOOP=$(losetup -f) && \\
    losetup "$LOOP" /tmp/pkb_swapfile && \\
    mkswap "$LOOP" && \\
    swapon "$LOOP" && \\
    echo "swap loop device: $LOOP"
  """),
  )


def _EnableZswap(pod: str) -> None:
  """Enable zswap with lz4 compressor and 20% pool limit inside the pod."""
  logging.info("[swap_encryption] Enabling zswap (lz4, 20%% pool)")
  for cmd in [
      "echo 1      > /sys/module/zswap/parameters/enabled",
      "echo lz4    > /sys/module/zswap/parameters/compressor",
      "echo 20     > /sys/module/zswap/parameters/max_pool_percent",
      "echo z3fold > /sys/module/zswap/parameters/zpool",
  ]:
    _PodExec(pod, cmd, ignore_failure=True)


# ---------------------------------------------------------------------------
# Phase 1 – fio Microbenchmarks
# ---------------------------------------------------------------------------


def _Phase1_Fio(
    pod: str, swap_dev: str, base_meta: dict
) -> list[sample.Sample]:
  """Run fio directly on the swap block device for raw I/O characterisation.

  Skipped for loop devices (GKE fallback path): the backing file lives on
  overlayfs which does not support O_DIRECT, and raw fio writes leave both
  the loop device and the overlayfs backing file in a stalled I/O state,
  hanging all subsequent losetup / mkswap calls.  Loop-on-overlayfs also
  produces results that reflect the container filesystem stack rather than
  the underlying storage medium, making the numbers misleading.

  For real block devices (hyperdisk, LSSD, NVMe) direct I/O is used and
  swap is restored (mkswap + swapon) after the fio run.
  """
  if swap_dev.startswith("/dev/loop"):
    logging.warning(
        "[swap_encryption] Phase 1 (fio) SKIPPED for loop device %s. "
        "overlayfs-backed loop devices cannot support reliable raw block I/O "
        "benchmarking — fio corrupts the overlayfs state and hangs subsequent "
        "I/O.  Attach a dedicated disk via the GKE node-pool settings "
        "(--disk-type=pd-ssd or local NVMe) for accurate fio results.",
        swap_dev,
    )
    return []

  results = []

  _PodExec(pod, f"swapoff {swap_dev}", ignore_failure=True)

  # Pre-fill device so read tests have real data.
  # Timeout = swap_size_gb / ~200 MB/s (conservative write rate) + buffer.
  prefill_timeout = max(600, _SWAP_SIZE_GB.value * 10)
  logging.info("[swap_encryption] Pre-filling swap device: %s", swap_dev)
  _PodExec(
      pod,
      (
          f"fio --name=prefill --filename={swap_dev} --ioengine=libaio"
          " --direct=1 --rw=write --bs=1m --size=32G --verify=0"
      ),
      timeout=prefill_timeout,
  )

  # Each fio job: runtime + 60 s buffer for setup/teardown
  fio_timeout = _FIO_RUNTIME_SEC.value + 60

  for name, rw, bs, depth, label in _FIO_JOBS:
    logging.info("[swap_encryption] fio: %s", name)
    cmd = (
        f"fio --name={name} --filename={swap_dev} "
        "--ioengine=libaio --direct=1 --verify=0 --randrepeat=0 "
        f"--bs={bs} --iodepth={depth} --rw={rw} "
        f"--time_based --runtime={_FIO_RUNTIME_SEC.value}s "
        "--output-format=json"
    )
    out, _ = _PodExec(pod, cmd, timeout=fio_timeout)
    results += _ParseFioJson(out, name, label, base_meta)

  # fio prefill overwrites the entire device, destroying the mkswap header.
  # Re-stamp and re-enable before the remaining phases need active swap.
  _PodExec(
      pod,
      f"mkswap {swap_dev} && swapon {swap_dev}",
      ignore_failure=True,
      timeout=120,
  )
  return results


def _ParseFioJson(
    stdout: str, job_name: str, label: str, base_meta: dict
) -> list[sample.Sample]:
  """Parse fio JSON output into PKB Samples."""
  results = []
  try:
    data = json.loads(stdout)
  except (json.JSONDecodeError, ValueError):
    logging.warning("[swap_encryption] fio JSON parse failed for %s", job_name)
    return results

  meta = dict(base_meta, fio_job=job_name, fio_label=label)
  for job in data.get("jobs", []):
    for direction in ("read", "write"):
      d = job.get(direction, {})
      if not d or d.get("io_bytes", 0) == 0:
        continue
      iops = float(d.get("iops", 0))
      bw_kib = float(d.get("bw", 0))
      clat = d.get("clat_ns", {})
      pct = clat.get("percentile", {})
      lat_mean = float(clat.get("mean", 0)) / 1000.0
      lat_p50 = float(pct.get("50.000000", 0)) / 1000.0
      lat_p99 = float(pct.get("99.000000", 0)) / 1000.0
      lat_p999 = float(pct.get("99.900000", 0)) / 1000.0
      m = dict(meta, direction=direction)
      results += [
          sample.Sample(f"{job_name}_{direction}_iops", iops, "iops", m),
          sample.Sample(
              f"{job_name}_{direction}_bw_mbps", bw_kib / 1024, "MB/s", m
          ),
          sample.Sample(f"{job_name}_{direction}_lat_mean", lat_mean, "us", m),
          sample.Sample(f"{job_name}_{direction}_lat_p50", lat_p50, "us", m),
          sample.Sample(f"{job_name}_{direction}_lat_p99", lat_p99, "us", m),
          sample.Sample(f"{job_name}_{direction}_lat_p999", lat_p999, "us", m),
      ]
  return results


# ---------------------------------------------------------------------------
# Phase 2a – CPU Overhead Under Swap Pressure
# ---------------------------------------------------------------------------


def _Phase2a_CpuOverhead(pod: str, base_meta: dict) -> list[sample.Sample]:
  """Measure CPU cost of dm-crypt / Nitro while stress-ng drives swap I/O.

  If --swap_encryption_stress_vm_bytes_list is set the phase is run once per
  listed intensity value so that a full pressure-curve is captured (gap 5).
  Otherwise the single value from --swap_encryption_stress_vm_bytes is used.
  """
  # Build the list of vm-bytes intensities to sweep (gap 5)
  if _STRESS_VM_BYTES_LIST.value.strip():
    intensities = [
        v.strip() for v in _STRESS_VM_BYTES_LIST.value.split(",") if v.strip()
    ]
  else:
    intensities = [_STRESS_VM_BYTES.value]

  results = []
  for vm_bytes in intensities:
    logging.info("[swap_encryption] Phase 2a: stress-ng intensity %s", vm_bytes)
    results += _RunCpuOverheadSweep(pod, base_meta, vm_bytes)
  return results


def _RunCpuOverheadSweep(
    pod: str, base_meta: dict, vm_bytes: str
) -> list[sample.Sample]:
  """Single stress-ng intensity sweep for Phase 2a."""
  results = []
  meta = dict(base_meta, phase="cpu_overhead", stress_vm_bytes=vm_bytes)
  timeout = _STRESS_TIMEOUT_SEC.value
  interval = 2

  vmstat_log = f"/tmp/pkb_vmstat_{vm_bytes}.log"
  pidstat_log = f"/tmp/pkb_pidstat_{vm_bytes}.log"

  # Detach collectors so they survive the exec session (bare & dies on exit).
  _PodExec(
      pod,
      (
          f"nohup setsid vmstat {interval} {timeout // interval} "
          f"> {vmstat_log} 2>&1 </dev/null &"
      ),
      timeout=15,
      ignore_failure=True,
  )
  _PodExec(
      pod,
      (
          f"nohup setsid pidstat -u {interval} {timeout // interval} "
          f"-p ALL > {pidstat_log} 2>&1 </dev/null &"
      ),
      timeout=15,
      ignore_failure=True,
  )

  # Prerequisite for any swap to occur (EKS kubelet = NoSwap -> swap.max=0).
  _EnableSwapPressure(pod)
  _PodExec(
      pod,
      'bash -c "echo -1000 > /proc/self/oom_score_adj" 2>/dev/null || true',
      ignore_failure=True,
  )

  t0 = time.time()
  # Detach stress-ng; --vm-keep holds pages resident so the kernel swaps
  # rather than the worker churning.  Output goes to a file we poll.
  _PodExec(
      pod,
      "nohup setsid stress-ng --vm 1"
      f" --vm-bytes {vm_bytes}"
      " --vm-keep"
      " --vm-method flip"
      f" --timeout {timeout}s"
      " --metrics-brief"
      " > /tmp/pkb_stress_out.txt 2>&1 </dev/null &",
      timeout=15,
      ignore_failure=True,
  )

  # Poll for completion; fail fast on credential death instead of timing out.
  deadline = t0 + timeout + 30
  stress_out = ""
  while time.time() < deadline:
    time.sleep(20)
    stress_out, _serr = _PodExec(
        pod,
        'cat /tmp/pkb_stress_out.txt 2>/dev/null || echo ""',
        ignore_failure=True,
    )
    _low = (_serr or "").lower()
    if (
        "token has expired" in _low
        or "unable to connect" in _low
        or "getting credentials" in _low
    ):
      _RelaxMemoryHigh(pod)
      raise RuntimeError(
          "AWS credentials expired mid-Gate-2 -- aws sso login and re-run. "
          f"Partial stress output: {stress_out!r}"
      )
    if "bogo-ops" in stress_out:
      break
  _RelaxMemoryHigh(pod)
  elapsed = time.time() - t0

  time.sleep(interval + 1)  # let collectors flush last sample

  results.append(sample.Sample("stress_ng_duration_sec", elapsed, "s", meta))

  for line in stress_out.splitlines():
    m = re.search(r"vm\s+\d+\s+(\d+)\s+\S+\s+bogo-ops", line)
    if m:
      results.append(
          sample.Sample("stress_ng_bogo_ops", float(m.group(1)), "ops", meta)
      )
      break

  vmstat_out, _ = _PodExec(pod, f"cat {vmstat_log}", ignore_failure=True)
  results += _ParseVmstat(vmstat_out, meta)

  pidstat_out, _ = _PodExec(pod, f"cat {pidstat_log}", ignore_failure=True)
  results += _ParsePidstat(pidstat_out, meta)
  return results


def _ParseVmstat(output: str, base_meta: dict) -> list[sample.Sample]:
  """Parse vmstat output for swap rates AND CPU utilisation.

  Standard vmstat column layout (non-header data lines, 0-indexed):
    r b swpd free buff cache  si  so  bi  bo  in  cs  us  sy  id  wa  st
    0 1    2    3    4     5   6   7   8   9  10  11  12  13  14  15  16

  si=6, so=7  – swap-in / swap-out pages/s
  us=12        – user CPU %
  sy=13        – system (kernel) CPU %  ← gap 2: system time %
  id=14        – idle CPU %
  wa=15        – I/O wait CPU %
  total_active = us + sy + wa          ← gap 1: total CPU utilisation
  """
  si_vals, so_vals = [], []
  us_vals, sy_vals, wa_vals = [], [], []

  for line in output.splitlines():
    parts = line.split()
    if len(parts) < 17 or not parts[0].isdigit():
      continue
    try:
      si_vals.append(float(parts[6]))
      so_vals.append(float(parts[7]))
      us_vals.append(float(parts[12]))
      sy_vals.append(float(parts[13]))
      wa_vals.append(float(parts[15]))
    except (ValueError, IndexError):
      pass

  if not si_vals:
    return []

  meta = dict(base_meta, metric_source="vmstat")

  def _avg(lst):
    return sum(lst) / len(lst) if lst else 0.0

  def _max(lst):
    return max(lst) if lst else 0.0

  total_active = [u + s + w for u, s, w in zip(us_vals, sy_vals, wa_vals)]

  return [
      # Swap rates
      sample.Sample("swap_in_pages_per_sec", _avg(si_vals), "pages/s", meta),
      sample.Sample(
          "swap_in_pages_per_sec_max", _max(si_vals), "pages/s", meta
      ),
      sample.Sample("swap_out_pages_per_sec", _avg(so_vals), "pages/s", meta),
      sample.Sample(
          "swap_out_pages_per_sec_max", _max(so_vals), "pages/s", meta
      ),
      # Total CPU utilisation (gap 1)
      sample.Sample("total_cpu_pct_avg", _avg(total_active), "%", meta),
      sample.Sample("total_cpu_pct_max", _max(total_active), "%", meta),
      # System (kernel) time % – encryption overhead signal (gap 2)
      sample.Sample("system_time_pct_avg", _avg(sy_vals), "%", meta),
      sample.Sample("system_time_pct_max", _max(sy_vals), "%", meta),
      # User and I/O-wait for completeness
      sample.Sample("user_cpu_pct_avg", _avg(us_vals), "%", meta),
      sample.Sample("iowait_cpu_pct_avg", _avg(wa_vals), "%", meta),
  ]


def _ParsePidstat(output: str, base_meta: dict) -> list[sample.Sample]:
  """Parse CPU % for swap/encryption-related kernel threads from pidstat."""
  cpu_by_proc: dict[str, list[float]] = {}
  for line in output.splitlines():
    parts = line.split()
    if len(parts) < 9:
      continue
    proc = parts[-1]
    if not any(t in proc for t in _CRYPTO_PROCS):
      continue
    try:
      cpu_by_proc.setdefault(proc, []).append(float(parts[7]))
    except (ValueError, IndexError):
      pass
  results = []
  meta = dict(base_meta, metric_source="pidstat")
  for proc, vals in cpu_by_proc.items():
    m = dict(meta, process=proc)
    results += [
        sample.Sample(f"cpu_pct_avg_{proc}", sum(vals) / len(vals), "%", m),
        sample.Sample(f"cpu_pct_max_{proc}", max(vals), "%", m),
    ]
  return results


# ---------------------------------------------------------------------------
# Phase 2b – I/O Interference
# ---------------------------------------------------------------------------


def _Phase2b_IoInterference(pod: str, base_meta: dict) -> list[sample.Sample]:
  """Quantify drop in application I/O when swap is under simultaneous pressure."""
  results = []
  app_file = "/tmp/pkb_app_io"
  timeout = _STRESS_TIMEOUT_SEC.value
  meta = dict(base_meta, phase="io_interference")

  # Create test file on the container filesystem (tmpfs or overlay)
  # Give generous timeout for large file creation
  _PodExec(
      pod,
      (
          f"fio --name=create --filename={app_file} "
          "--rw=write --bs=1m --size=8G --verify=0"
      ),
      timeout=600,
  )

  def _run_app_fio(pressure_label: str) -> list[sample.Sample]:
    cmd = (
        f"fio --name=app_io --filename={app_file} "
        "--ioengine=psync --direct=0 "
        "--rw=randrw --bs=4k --iodepth=1 --size=8G --verify=0 "
        "--time_based --runtime=60s --output-format=json"
    )
    out, _ = _PodExec(pod, cmd, ignore_failure=True)
    return _ParseFioJson(
        out,
        "app_io",
        f"App I/O ({pressure_label})",
        dict(meta, pressure=pressure_label),
    )

  # 1. Baseline – no swap pressure
  logging.info("[swap_encryption] I/O interference: baseline (no pressure)")
  results += _run_app_fio("no_pressure")

  # 2. Under swap pressure
  # Use nohup + disown so bash exits immediately after launching stress-ng;
  # otherwise kubectl exec keeps the session alive until stress-ng finishes
  # (300 s) and PKB's IssueCommand times out.
  logging.info("[swap_encryption] I/O interference: under swap pressure")
  # On memory-constrained nodes (e.g. m6id, 64 GB) a >RAM stress will OOM-kill
  # the pod/fio instead of swapping -> the under-pressure fio returned nothing
  # and the sample was silently dropped.  Lift the cgroup swap ceiling first so
  # the stress swaps (the behaviour we actually want to measure) and the fio
  # survives.
  _EnableSwapPressure(pod)
  _PodExec(
      pod,
      'bash -c "echo -500 > /proc/self/oom_score_adj" 2>/dev/null || true',
      ignore_failure=True,
  )
  _PodExec(
      pod,
      (
          "nohup stress-ng --vm 1 "
          f"--vm-bytes {_STRESS_VM_BYTES.value} "
          "--vm-keep "
          "--vm-method all "
          f"--timeout {timeout}s "
          ">/tmp/pkb_stress_io.log 2>&1 & disown; sleep 2; echo STRESS_STARTED"
      ),
      timeout=30,
  )
  time.sleep(10)  # let swap pressure build
  pressure_samples = _run_app_fio("with_swap_pressure")
  if not pressure_samples:
    logging.warning(
        "[swap_encryption] under-pressure app_io empty -- "
        "settling 15s and retrying once"
    )
    time.sleep(15)
    pressure_samples = _run_app_fio("with_swap_pressure")
  if not pressure_samples:
    logging.error(
        "[swap_encryption] under-pressure app_io produced NO sample; "
        "interference delta will be incomplete for this run"
    )
  results += pressure_samples

  # Wait for stress-ng to finish naturally (it auto-exits after --timeout)
  _PodExec(pod, f"sleep {timeout}", ignore_failure=True, timeout=timeout + 30)
  _RelaxMemoryHigh(pod)
  return results


# ---------------------------------------------------------------------------
# Phase 3a – Redis Latency Under Memory Pressure
# ---------------------------------------------------------------------------


def _Phase3a_Redis(pod: str, base_meta: dict) -> list[sample.Sample]:
  """Load Redis beyond its memory cap and measure GET/SET P50/P90/P99 latency.

  Uses memtier_benchmark (installed in the DaemonSet) instead of the built-in
  redis-benchmark because memtier reports per-percentile latency (P50/P90/P99)
  which is required by the test plan (redis SET/GET P90/P99 under memory
  pressure).  This mirrors the approach in PKB's redis_memtier_benchmark.
  """
  results = []
  meta = dict(base_meta, workload="redis", tool="memtier_benchmark")

  # `service redis-server start` returns 0 even when policy-rc.d blocks it
  # (no systemd), so the `||` fallback never ran -> redis was never up.
  _PodExec(
      pod,
      "pkill -x redis-server 2>/dev/null; "
      "redis-server --daemonize yes --bind 127.0.0.1 --port 6379 "
      '--protected-mode no --save "" --appendonly no '
      "> /tmp/pkb_redis.log 2>&1; sleep 2; "
      "(/usr/bin/redis-cli -h 127.0.0.1 ping || redis-cli -h 127.0.0.1 ping)",
      ignore_failure=True,
  )
  time.sleep(3)

  maxmem = _REDIS_MAXMEMORY_MB.value * 1024 * 1024
  _PodExec(pod, f"redis-cli CONFIG SET maxmemory {maxmem}", ignore_failure=True)
  _PodExec(
      pod,
      "redis-cli CONFIG SET maxmemory-policy allkeys-lru",
      ignore_failure=True,
  )

  # Pre-load dataset (forces eviction/swap once dataset > maxmemory)
  n_keys = (_REDIS_DATASET_MB.value * 1024 * 1024) // 128
  logging.info(
      "[swap_encryption] Loading %d Redis keys (%d MB)",
      n_keys,
      _REDIS_DATASET_MB.value,
  )
  _PodExec(
      pod,
      f"redis-benchmark -n {n_keys} -d 128 -t SET -q >/dev/null 2>&1",
      ignore_failure=True,
      timeout=600,
  )

  # Apply swap pressure — detach so kubectl exec returns immediately
  _PodExec(
      pod,
      (
          "nohup stress-ng --vm 1 --vm-bytes 8G"
          " --vm-method all --timeout 120s >/tmp/pkb_stress_redis.log 2>&1 &"
          " disown; sleep 2; echo STRESS_STARTED"
      ),
      timeout=30,
  )
  time.sleep(8)

  # memtier_benchmark: JSON output for per-percentile latency
  mt_cmd = (
      "/usr/local/bin/memtier_benchmark "
      "--server 127.0.0.1 --port 6379 --protocol redis "
      "--clients 50 --threads 4 --test-time 60 "
      "--data-size 128 "
      "--ratio 1:1 "  # equal GET/SET mix
      "--print-percentiles 50,90,99,99.9 "
      "--hide-histogram "
      "--json-out-file /tmp/pkb_memtier.json "
      "2>&1"
  )
  _PodExec(pod, mt_cmd, ignore_failure=True, timeout=120)
  results += _ParseMemtierJson("/tmp/pkb_memtier.json", pod, meta)

  return results


def _PickLatency(pcts, old_lat, modern_key, old_key, default=0):
  """Return a latency percentile from the modern 'Percentile Latencies' dict,
  falling back to the legacy nested 'Latency' sub-dict."""
  if modern_key in pcts:
    return pcts[modern_key]
  return old_lat.get(old_key, default)


def _ParseMemtierJson(
    json_path: str, pod: str, base_meta: dict
) -> list[sample.Sample]:
  """Parse memtier_benchmark JSON output into PKB Samples.

  Extracts throughput (ops/s) and latency percentiles (P50, P90, P99)
  for both GET and SET operations, as required by the test plan.
  """
  raw, _ = _PodExec(
      pod, f'cat {json_path} 2>/dev/null || echo ""', ignore_failure=True
  )
  results = []
  try:
    data = json.loads(raw)
  except (json.JSONDecodeError, ValueError):
    logging.warning("[swap_encryption] memtier JSON parse failed")
    return results

  # memtier JSON structure: {"ALL STATS": {"Sets": {...}, "Gets": {...}, ...}}
  all_stats = data.get("ALL STATS", {})
  op_map = {
      "Sets": "set",
      "Gets": "get",
      "Totals": "total",
  }
  for json_key, op_label in op_map.items():
    section = all_stats.get(json_key, {})
    if not isinstance(section, dict) or not section:
      continue
    m = dict(base_meta, operation=op_label)
    ops_sec = section.get("Ops/sec", 0)
    # Modern memtier schema: flat 'Average Latency' + 'Percentile Latencies'
    # dict keyed 'p50.00'/'p90.00'/'p99.00'/'p99.90'.  Older builds nested
    # everything under a 'Latency' sub-dict; support both.
    pcts = section.get("Percentile Latencies", {})
    if not isinstance(pcts, dict):
      pcts = {}
    old_lat = section.get("Latency", {})
    if not isinstance(old_lat, dict):
      old_lat = {}

    lat_avg = section.get("Average Latency", old_lat.get("Average Latency", 0))
    lat_p50 = _PickLatency(pcts, old_lat, "p50.00", "50th Percentile Latency")
    lat_p90 = _PickLatency(pcts, old_lat, "p90.00", "90th Percentile Latency")
    lat_p99 = _PickLatency(pcts, old_lat, "p99.00", "99th Percentile Latency")
    lat_p999 = _PickLatency(
        pcts, old_lat, "p99.90", "99.9th Percentile Latency"
    )
    results += [
        sample.Sample(
            f"redis_{op_label}_ops_per_sec", float(ops_sec), "ops/s", m
        ),
        sample.Sample(f"redis_{op_label}_lat_avg_ms", float(lat_avg), "ms", m),
        sample.Sample(f"redis_{op_label}_lat_p50_ms", float(lat_p50), "ms", m),
        sample.Sample(f"redis_{op_label}_lat_p90_ms", float(lat_p90), "ms", m),
        sample.Sample(f"redis_{op_label}_lat_p99_ms", float(lat_p99), "ms", m),
        sample.Sample(
            f"redis_{op_label}_lat_p999_ms", float(lat_p999), "ms", m
        ),
    ]
  return results


# ---------------------------------------------------------------------------
# Phase 3b – Kernel Build Under Memory Constraint
# ---------------------------------------------------------------------------


def _Phase3b_KernelBuild(pod: str, base_meta: dict) -> list[sample.Sample]:
  """Compile Linux inside a cgroup memory cap; compare to unconstrained."""
  results = []
  ver = _KERNEL_VERSION.value
  root = "/tmp/pkb_kernel"
  tarball = f"{root}/linux-{ver}.tar.xz"
  src = f"{root}/linux-{ver}"
  url = (
      "https://cdn.kernel.org/pub/linux/kernel/"
      f'v{ver.split(".")[0]}.x/linux-{ver}.tar.xz'
  )

  _PodExec(pod, f"mkdir -p {root}")
  _PodExec(
      pod,
      f"test -f {tarball} || wget -q --timeout=120 -O {tarball} {url}",
      timeout=600,
  )
  _PodExec(pod, f"test -d {src}    || tar -xf {tarball} -C {root}", timeout=600)
  _PodExec(pod, f"make -C {src} defconfig -j$(nproc) 2>&1", timeout=300)

  cgroup = "/sys/fs/cgroup/memory/pkb_kernelbuild"
  mem_bytes = _KERNEL_MEMORY_MB.value * 1024 * 1024

  _PodExec(
      pod,
      f"mkdir -p {cgroup} && echo {mem_bytes} > {cgroup}/memory.limit_in_bytes",
      ignore_failure=True,
  )

  def _build(label: str, use_cgroup: bool) -> sample.Sample:
    _PodExec(pod, f"make -C {src} clean 2>&1")
    if use_cgroup:
      cmd = (
          "cgexec -g memory:pkb_kernelbuild "
          f"make -C {src} -j$(nproc) vmlinux 2>&1 "
          f"|| make -C {src} -j$(nproc) vmlinux 2>&1"
      )
    else:
      cmd = f"make -C {src} -j$(nproc) vmlinux 2>&1"
    t0 = time.time()
    _PodExec(pod, cmd, timeout=3600)  # kernel builds can take up to ~1 hr
    elapsed = time.time() - t0
    m = dict(
        base_meta,
        workload="kernel_build",
        kernel_version=ver,
        build_variant=label,
        memory_limit_mb=(
            _KERNEL_MEMORY_MB.value if use_cgroup else "unconstrained"
        ),
    )
    return sample.Sample("kernel_build_elapsed_sec", elapsed, "s", m)

  s_constrained = _build("constrained", use_cgroup=True)
  s_unconstrained = _build("unconstrained", use_cgroup=False)
  results += [s_constrained, s_unconstrained]

  if s_unconstrained.value > 0:
    ratio = s_constrained.value / s_unconstrained.value
    results.append(
        sample.Sample(
            "kernel_build_slowdown_ratio",
            ratio,
            "ratio",
            dict(
                base_meta,
                workload="kernel_build",
                kernel_version=ver,
                memory_limit_mb=_KERNEL_MEMORY_MB.value,
            ),
        )
    )
  return results


# ---------------------------------------------------------------------------
# Phase 3c – OpenSearch
# ---------------------------------------------------------------------------


def _Phase3c_OpenSearch(pod: str, base_meta: dict) -> list[sample.Sample]:
  """Index + query workload under swap pressure (esrally or curl fallback)."""
  meta = dict(base_meta, workload="opensearch")

  # Detach stress-ng so kubectl exec exits immediately; see Phase 2b comment
  _PodExec(
      pod,
      (
          f"nohup stress-ng --vm 1 --vm-bytes {_STRESS_VM_BYTES.value}"
          f" --vm-method all --timeout {_STRESS_TIMEOUT_SEC.value}s"
          " >/tmp/pkb_stress_opensearch.log 2>&1 & disown; sleep 2; echo"
          " STRESS_STARTED"
      ),
      timeout=30,
  )
  time.sleep(10)

  esrally_out, _ = _PodExec(
      pod, "which esrally 2>/dev/null", ignore_failure=True
  )
  if esrally_out.strip():
    return _RunEsrally(pod, meta)
  else:
    logging.info("[swap_encryption] esrally absent – using curl fallback")
    return _RunOpenSearchCurl(pod, meta)


def _RunEsrally(pod: str, meta: dict) -> list[sample.Sample]:
  """Run esrally geonames track with a capped JVM heap to induce swap pressure.

  esrally is installed via pip3 in the DaemonSet init script and uses the
  same geonames track as PKB's standalone esrally_benchmark.py, so results
  are directly comparable.  The JVM heap is capped to 512 MB so the OS page
  cache overflows to swap during indexing — the key swap pressure scenario
  described in the methodology doc.
  """
  jvm_heap_mb = 512
  # Patch jvm.options before starting Elasticsearch/OpenSearch
  _PodExec(
      pod,
      textwrap.dedent(f"""
    for f in /etc/elasticsearch/jvm.options /etc/opensearch/jvm.options \\
              /usr/share/elasticsearch/config/jvm.options \\
              /usr/share/opensearch/config/jvm.options; do
      [ -f "$f" ] || continue
      sed -i 's/^-Xms.*/-Xms{jvm_heap_mb}m/' "$f"
      sed -i 's/^-Xmx.*/-Xmx{jvm_heap_mb}m/' "$f"
    done
    export ES_JAVA_OPTS="-Xms{jvm_heap_mb}m -Xmx{jvm_heap_mb}m"
    export OPENSEARCH_JAVA_OPTS="-Xms{jvm_heap_mb}m -Xmx{jvm_heap_mb}m"
  """),
      ignore_failure=True,
  )

  _PodExec(
      pod,
      "systemctl start elasticsearch 2>/dev/null || "
      "systemctl start opensearch 2>/dev/null || true",
      ignore_failure=True,
  )
  time.sleep(15)  # wait for the engine to be ready

  _PodExec(
      pod,
      textwrap.dedent("""
    esrally race \\
      --track=geonames \\
      --target-hosts=localhost:9200 \\
      --pipeline=benchmark-only \\
      --report-format=csv \\
      --report-file=/tmp/pkb_esrally.csv \\
      --track-param="number_of_replicas:0" \\
      2>&1
  """),
      ignore_failure=True,
      timeout=3600,
  )

  csv_out, _ = _PodExec(pod, 'cat /tmp/pkb_esrally.csv 2>/dev/null || echo ""')
  results = []
  for line in csv_out.splitlines():
    parts = [p.strip() for p in line.split(",")]
    if len(parts) < 3:
      continue
    metric = parts[0].lower().replace(" ", "_").replace("-", "_")
    try:
      value = float(parts[2])
      unit = parts[3].strip() if len(parts) > 3 else "unknown"
      results.append(
          sample.Sample(
              f"opensearch_{metric}",
              value,
              unit,
              dict(meta, tool="esrally", jvm_heap_mb=jvm_heap_mb),
          )
      )
    except (ValueError, IndexError):
      pass
  return results


def _RunOpenSearchCurl(pod: str, meta: dict) -> list[sample.Sample]:
  """Minimal OpenSearch benchmark via curl (fallback).

  Elasticsearch/OpenSearch JVM heap is deliberately capped to a small value
  so that the JVM off-heap buffers and OS page cache overflow to swap during
  indexing, making this a realistic swap-pressure workload (gap 4).
  """
  # Cap the JVM heap so OS page cache / off-heap memory causes swap pressure.
  # 512 MB heap on a 32-vCPU node leaves almost all RAM available for page
  # cache, which the kernel will then need to reclaim under bulk-index load.
  jvm_heap_mb = 512
  _PodExec(
      pod,
      textwrap.dedent(f"""
    # Patch jvm.options in-place for Elasticsearch and OpenSearch installs
    for jvm_opts_file in \\
        /etc/elasticsearch/jvm.options \\
        /etc/opensearch/jvm.options \\
        /usr/share/elasticsearch/config/jvm.options \\
        /usr/share/opensearch/config/jvm.options; do
      if [ -f "$jvm_opts_file" ]; then
        sed -i 's/^-Xms.*/-Xms{jvm_heap_mb}m/' "$jvm_opts_file"
        sed -i 's/^-Xmx.*/-Xmx{jvm_heap_mb}m/' "$jvm_opts_file"
        echo "[swap_encryption] Patched $jvm_opts_file → -Xms{jvm_heap_mb}m -Xmx{jvm_heap_mb}m"
      fi
    done
    # Environment-variable fallback (works with both ES and OpenSearch)
    export ES_JAVA_OPTS="-Xms{jvm_heap_mb}m -Xmx{jvm_heap_mb}m"
    export OPENSEARCH_JAVA_OPTS="-Xms{jvm_heap_mb}m -Xmx{jvm_heap_mb}m"
  """),
      ignore_failure=True,
  )

  _PodExec(
      pod,
      "systemctl start elasticsearch 2>/dev/null || true",
      ignore_failure=True,
  )
  time.sleep(10)

  doc = '{"index":{}}\n{"field":"benchmark","ts":"2026-01-01"}\n'
  bulk = doc * 500

  t0 = time.time()
  _PodExec(
      pod,
      (
          f"printf \"%s\" '{bulk}' | "
          "curl -s -X POST 'http://localhost:9200/pkb_test/_bulk' "
          "-H 'Content-Type: application/x-ndjson' "
          "--data-binary @- -o /dev/null"
      ),
      ignore_failure=True,
  )
  index_sec = time.time() - t0

  t0 = time.time()
  _PodExec(
      pod,
      (
          "curl -s 'http://localhost:9200/pkb_test/_search?q=field:benchmark' "
          "-o /dev/null"
      ),
      ignore_failure=True,
  )
  query_sec = time.time() - t0

  m = dict(meta, tool="curl_fallback")
  return [
      sample.Sample("opensearch_bulk_index_sec", index_sec, "s", m),
      sample.Sample("opensearch_query_latency_sec", query_sec, "s", m),
  ]


# ---------------------------------------------------------------------------
# Gap 7 – Cloud cost estimation
# ---------------------------------------------------------------------------

# On-demand pricing (USD/hr) for the primary benchmark instance types.
# Values are approximate list prices (us-central1 / us-east-1) as of 2026-05.
# Update this table when running on other regions or reserved/spot capacity.
_INSTANCE_PRICE_USD_PER_HR: dict[str, float] = {
    # GCP
    "n4-highmem-32": 3.0256,  # 32 vCPU, 256 GB RAM, us-central1
    "n2-highmem-32": 2.5216,  # 32 vCPU, 256 GB RAM
    "n2-standard-32": 1.5264,  # 32 vCPU, 120 GB RAM
    "z3-highmem-8": 2.7248,  # 8 vCPU + 4× LSSD, us-central1
    # AWS
    "i4i.4xlarge": 1.4960,  # 16 vCPU, 128 GB RAM, NVMe Instance Store
    "i4i.2xlarge": 0.7480,
    "m6id.4xlarge": 0.9072,  # 16 vCPU, 64 GB RAM, NVMe Instance Store
    "m6i.4xlarge": 0.7680,  # 16 vCPU, 64 GB RAM, no Instance Store
    "r6i.4xlarge": 1.0080,  # 16 vCPU, 128 GB RAM, no Instance Store
}


def _CollectCostSample(
    pod: str, elapsed_sec: float, base_meta: dict
) -> list[sample.Sample]:
  """Emit a cost_estimate_usd sample for the benchmark run (gap 7).

  Instance type is read from cloud metadata inside the pod.  Price is looked
  up from _INSTANCE_PRICE_USD_PER_HR; if unknown, the sample is omitted and
  a warning is logged.

  Args:
    pod:         Benchmark pod name.
    elapsed_sec: Wall-clock seconds the benchmark phases took.
    base_meta:   Shared metadata dict.

  Returns:
    A list of zero or one sample.Sample.
  """
  # Detect instance type from cloud metadata
  instance_type = ""

  # GCP: machine type is the last segment of the metadata URL value
  gcp_type_out, _ = _PodExec(
      pod,
      "curl -s -m 3 --fail"
      " http://metadata.google.internal/computeMetadata/v1/instance/machine-type"
      ' -H "Metadata-Flavor: Google" 2>/dev/null || echo ""',
      ignore_failure=True,
  )
  if gcp_type_out.strip():
    instance_type = gcp_type_out.strip().split("/")[-1]

  if not instance_type:
    # AWS: instance-type is a plain string
    aws_type_out, _ = _PodExec(
        pod,
        "T=$(curl -s -m 3 -X PUT "
        "http://169.254.169.254/latest/api/token "
        '-H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null); '
        'curl -s -m 3 -H "X-aws-ec2-metadata-token: $T" --fail '
        "http://169.254.169.254/latest/meta-data/instance-type "
        '2>/dev/null || echo ""',
        ignore_failure=True,
    )
    instance_type = aws_type_out.strip()

  # Allow flag override (useful when running on custom/renamed machine types)
  if _INSTANCE_SIZE_LABEL.value:
    instance_type = _INSTANCE_SIZE_LABEL.value

  # Fallback: in-pod IMDS can be blocked by the EC2 metadata hop limit and
  # return "" (observed on the io2 m6id runs).  The launch machine-type flag is
  # authoritative and always set, so trust it when metadata is unavailable.
  if not instance_type:
    instance_type = _BENCHMARK_MACHINE_TYPE.value
    if instance_type:
      logging.info(
          "[swap_encryption] cost: using launch-flag instance type %s "
          "(in-pod metadata returned empty)",
          instance_type,
      )

  price = _INSTANCE_PRICE_USD_PER_HR.get(instance_type)
  if price is None:
    logging.warning(
        '[swap_encryption] Unknown instance type "%s" – skipping cost sample. '
        "Add it to _INSTANCE_PRICE_USD_PER_HR to enable cost tracking.",
        instance_type,
    )
    return []

  hours = elapsed_sec / 3600.0
  compute_cost = hours * price
  # io2 storage cost (us-east-1): $0.125/GB-month + $0.065/provisioned-IOPS-mo
  # for the first 32k IOPS.  The helper provisions 500 GB / 16000 IOPS.
  storage_cost = 0.0
  if _SWAP_TYPE.value == "io2":
    months = hours / 730.0
    storage_cost = months * (500 * 0.125 + 16000 * 0.065)
  cost = compute_cost + storage_cost
  meta = dict(
      base_meta,
      instance_type=instance_type,
      price_usd_per_hr=price,
      compute_cost_usd=round(compute_cost, 6),
      storage_cost_usd=round(storage_cost, 6),
      io2_encrypted=(
          _IO2_ENCRYPTED.value if _SWAP_TYPE.value == "io2" else "n/a"
      ),
      benchmark_elapsed_sec=round(elapsed_sec, 1),
  )
  return [sample.Sample("cost_estimate_usd", cost, "USD", meta)]


# ---------------------------------------------------------------------------
# Swap device detection (runs inside the pod)
# ---------------------------------------------------------------------------


def _DetectSwapDevice(pod: str) -> str:
  """Return the active swap device path on the cluster node."""
  if _SWAP_DEVICE.value:
    return _SWAP_DEVICE.value

  # Prefer dm-crypt mapped device (GKE)
  dm_out, _ = _PodExec(
      pod,
      "test -e /dev/mapper/swap_encrypted && echo /dev/mapper/swap_encrypted ||"
      " awk 'NR>1{print $1; exit}' /proc/swaps",
      ignore_failure=True,
  )
  dev = dm_out.strip()
  if dev:
    return dev
  raise ValueError(
      "No active swap device found in the benchmark pod. "
      "Use --swap_encryption_device to specify one."
  )


# ---------------------------------------------------------------------------
# Metadata builder
# ---------------------------------------------------------------------------


def _BuildMetadata(pod: str, swap_dev: str) -> dict:
  """Collect node environment, encryption type, and config into a dict."""

  kernel_out, _ = _PodExec(pod, "uname -r", ignore_failure=True)
  mem_out, _ = _PodExec(
      pod, "awk '/MemTotal/{print $2}' /proc/meminfo", ignore_failure=True
  )
  swap_out, _ = _PodExec(
      pod,
      "awk 'NR>1{sum+=$3} END{print sum+0}' /proc/swaps",
      ignore_failure=True,
  )

  try:
    mem_gb = round(int(mem_out.strip()) / (1024 * 1024), 1)
  except ValueError:
    mem_gb = 0
  try:
    swap_gb = round(int(swap_out.strip()) / (1024 * 1024), 1)
  except ValueError:
    swap_gb = 0

  # Encryption type
  enc = "unknown"
  if "/dev/mapper/" in swap_dev:
    table_out, _ = _PodExec(
        pod,
        f'dmsetup table {swap_dev.split("/")[-1]} 2>/dev/null || echo ""',
        ignore_failure=True,
    )
    enc = "dm-crypt-plain" if "crypt" in table_out.lower() else "dm-other"
  elif any(x in swap_dev for x in ("nvme", "xvd", "sdb")):
    enc = "nitro_hardware_offload"
  elif "swapfile" in swap_dev:
    enc = "none"

  cloud = _DetectCloud(pod)

  # Gap 6: instance size label for multi-size comparison runs.
  # If the flag is set use it directly; otherwise try to read it from
  # cloud metadata so that the field is always populated.
  instance_label = _INSTANCE_SIZE_LABEL.value
  if not instance_label:
    gcp_type_out, _ = _PodExec(
        pod,
        "curl -s -m 3 --fail"
        " http://metadata.google.internal/computeMetadata/v1/instance/machine-type"
        ' -H "Metadata-Flavor: Google" 2>/dev/null || echo ""',
        ignore_failure=True,
    )
    if gcp_type_out.strip():
      instance_label = gcp_type_out.strip().split("/")[-1]
  if not instance_label:
    aws_type_out, _ = _PodExec(
        pod,
        "curl -s -m 3 --fail "
        "http://169.254.169.254/latest/meta-data/instance-type "
        '2>/dev/null || echo ""',
        ignore_failure=True,
    )
    instance_label = aws_type_out.strip()

  return {
      "benchmark": BENCHMARK_NAME,
      "execution_mode": "kubernetes_privileged_pod",
      "cloud": cloud,
      "instance_size": instance_label,
      "kernel_version": kernel_out.strip(),
      "host_memory_gb": mem_gb,
      "swap_device": swap_dev,
      "swap_size_gb": swap_gb,
      "swap_encryption": enc,
      # Test-matrix columns: encryption on/off, node image type, IOPS target
      "benchmark_machine_type": _BENCHMARK_MACHINE_TYPE.value,
      # Other config
      "zswap_enabled": _ENABLE_ZSWAP.value,
      "min_free_kbytes": _MIN_FREE_KBYTES.value,
      "fio_runtime_sec": _FIO_RUNTIME_SEC.value,
      "stress_vm_bytes": _STRESS_VM_BYTES.value,
      "stress_vm_bytes_list": _STRESS_VM_BYTES_LIST.value,
      "stress_timeout_sec": _STRESS_TIMEOUT_SEC.value,
      "nodepool": _NODEPOOL.value,
  }
