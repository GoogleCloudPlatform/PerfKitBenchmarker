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
                 swap device: /dev/mapper/swap_encrypted (over dedicated
                 hyperdisk or LSSD RAID-0 /dev/md0).
                 Single-disk fallback: plain loop device on
                 /mnt/stateful_partition — dm-crypt is blocked by COS
                 kernel namespace restrictions from inside a pod.

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
import textwrap
import time
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
  flags: {}
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
    ["auto", "hyperdisk", "lssd", "boot_disk", "instance_store", "io2"],
    "Swap backing storage target, one per methodology test-matrix row:\n"
    "  GKE:  boot_disk (swap file on the OS boot disk — pd-balanced or "
    "hyperdisk-balanced, chosen via --swap_encryption_boot_disk_type),\n"
    "        hyperdisk (dedicated hyperdisk-balanced data disk),\n"
    "        lssd (dedicated Local SSD RAID-0).\n"
    "  AWS:  instance_store (NVMe Instance Store, Nitro-encrypted),\n"
    "        io2 (EBS io2 data/root volume).\n"
    "dm-crypt is applied on the GKE targets when "
    "--swap_encryption_enable_dmcrypt is set; AWS targets are encrypted by "
    "Nitro at the hardware level.  auto = detect from cloud + instance type.",
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
    "Combined stress-ng working-set size (total in-flight footprint, not "
    "per-worker).  It is divided equally across --swap_encryption_stress_vm_"
    "workers before being passed to stress-ng, so the total memory touched "
    "equals this value.  Should exceed node RAM to force kernel swapping.",
)


_STRESS_VM_BYTES_LIST = flags.DEFINE_string(
    "swap_encryption_stress_vm_bytes_list",
    "",
    "Comma-separated list of stress-ng --vm-bytes values to iterate over "
    'in Phase 2a CPU-overhead sweeps, e.g. "14G,21G,28G".  When non-empty '
    "this overrides --swap_encryption_stress_vm_bytes and Phase 2a is run "
    "once per entry so that the swap-pressure intensity curve is captured.",
)


_STRESS_VM_WORKERS = flags.DEFINE_integer(
    "swap_encryption_stress_vm_workers",
    4,
    "Number of parallel stress-ng --vm workers for Phase 2a.  The total "
    "working set (the autoscaled vm_bytes) is divided equally across workers, "
    "so the combined footprint stays under RAM+swap (no OOM) while exceeding "
    "RAM (forcing swap).  Multiple workers are needed for fill speed — a "
    "single write64 worker cannot dirty enough memory within the timeout to "
    "reach RAM (run swap1: ~184 GB resident, no swap).  To stop the N "
    "workers' resident sets from collapsing to one worker's share, the "
    "stressor uses random access (rand-set) and disables KSM page-merging "
    "(without those, identical write64 pages across workers were merged, "
    "leaving only ~vm_bytes/N resident and swap_out ~0).",
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
    "--swap_encryption_swap_type=io2 on AWS/EKS.",
)


_IO2_KMS_KEY_ID = flags.DEFINE_string(
    "swap_encryption_io2_kms_key_id",
    "",
    "Optional KMS key id/ARN for the encrypted io2 volume. Empty = the "
    "account default aws/ebs key. Ignored unless io2_encrypted is True.",
)


_FAIL_ON_DEGRADED = flags.DEFINE_boolean(
    "swap_encryption_fail_on_degraded",
    True,
    "When True (default), raise an error at the end of Run() if the run was "
    "catastrophically degraded — e.g. the benchmark pod was OOM-evicted and "
    "replaced mid-run, Gate 1 (fio) produced no samples, or the stress-ng "
    "swap-pressure phase was OOM-killed before completing.  This prevents PKB "
    "from reporting SUCCEEDED for a run whose post-eviction phases produced "
    "empty or meaningless data.  Set False to keep the legacy behaviour of "
    "always returning whatever partial samples were collected.",
)


_PHASES = flags.DEFINE_list(
    "swap_encryption_phases",
    ["all"],
    "Which Run() phases to execute, for fast iteration against an "
    "already-provisioned cluster (e.g. --run_stage=run --run_uri=...).  "
    "Comma-separated subset of: fio (Tier 1 microbenchmarks), 2a (stress-ng "
    "CPU overhead + swap pressure), 2b (I/O interference), 3a (redis), "
    '3b (kernel build), 3c (opensearch).  Default "all" runs everything.  '
    "Example: --swap_encryption_phases=2a runs only the swap-pressure phase. "
    "Phases not listed are skipped and do not affect the degraded-run gate "
    '(e.g. skipping fio will not be reported as "Gate 1 produced no samples").',
)


_MIN_SWAP_OUT_PAGES = flags.DEFINE_integer(
    "swap_encryption_min_swap_out_pages",
    1000,
    "Minimum peak swap-out rate (pages/s) that Phase 2a must reach for the run"
    " to count as a real swap-encryption measurement.  Below this the working"
    " set never meaningfully paged (e.g. run swap1 peaked at 176 pages/s of"
    ' noise yet "passed" the old zero-only gate), so the dm-crypt overhead is'
    " hollow and the run is flagged degraded.  A genuinely swapping run peaks"
    " in the tens-to-hundreds of thousands of pages/s.  Set 0 to accept any"
    " non-zero swap-out (legacy behaviour).",
)


_BENCHMARK_MACHINE_TYPE = flags.DEFINE_string(
    "swap_encryption_benchmark_machine_type",
    "n4-highmem-32",
    "Machine type for the benchmark nodepool created in Prepare(). "
    "Use n4-highmem-32 (hyperdisk, default) or c4-standard-8-lssd "
    "(LSSD RAID-0).  The matching swap setup is selected automatically.",
)


_BENCHMARK_LSSD = flags.DEFINE_boolean(
    "swap_encryption_lssd",
    False,
    "Force LSSD RAID-0 swap path even when the machine type name does not "
    'contain "lssd".  Auto-detected from machine type when False.',
)


_LSSD_COUNT = flags.DEFINE_integer(
    "swap_encryption_lssd_count",
    1,
    "Number of local NVMe SSDs to attach as raw block devices "
    "(--local-nvme-ssd-block count=N).  Must match the fixed local SSD "
    "count for the chosen machine type: c4-standard-8-lssd=1, "
    "c4-standard-16-lssd=2, i4i.4xlarge has NVMe Instance Store (AWS).  "
    "Default 1 covers most single-lssd machine types.",
)


_ENABLE_DMCRYPT = flags.DEFINE_boolean(
    "swap_encryption_enable_dmcrypt",
    True,
    "When True (default), configure dm-crypt on the swap device — the "
    '"encryption enabled" column of the test matrix.  Set False to use '
    "plain swap (encryption disabled column).",
)


_NODE_IMAGE_TYPE = flags.DEFINE_string(
    "swap_encryption_node_image_type",
    "UBUNTU_CONTAINERD",
    "GKE node image type for the benchmark nodepool.  "
    "UBUNTU_CONTAINERD is required for dm-crypt measurement: COS locks "
    "down device-mapper at the kernel LSM level and cryptsetup hangs "
    "indefinitely from any pod context (even privileged, even via nsenter "
    "into the host mount namespace).  Ubuntu GKE nodes allow cryptsetup "
    "from privileged pods without restriction.  "
    "Use COS_CONTAINERD only when dm-crypt is disabled "
    "(--noswap_encryption_enable_dmcrypt) to measure plain-swap overhead.  "
    "AL2 on EKS.",
)


_BOOT_DISK_TYPE = flags.DEFINE_string(
    "swap_encryption_boot_disk_type",
    "hyperdisk-balanced",
    "Disk type for the benchmark nodepool boot disk.  Use hyperdisk-balanced "
    "for production machines (n4, c3, c4 families).  Use pd-ssd for n2/e2 "
    "dev/test machines, which do not support hyperdisk-balanced.",
)


_BOOT_DISK_IOPS = flags.DEFINE_integer(
    "swap_encryption_boot_disk_iops",
    80000,
    "Provisioned IOPS for the boot disk (hyperdisk-balanced only).  "
    "80 000 is the COS max-IOPS target.  Ignored for pd-ssd.",
)


_BOOT_DISK_THROUGHPUT = flags.DEFINE_integer(
    "swap_encryption_boot_disk_throughput",
    1200,
    "Provisioned throughput in MB/s for the boot disk (hyperdisk-balanced "
    "only).  Must be set together with iops.  1200 MB/s pairs with 80 000 "
    "IOPS for production; use 140 (minimum) for dev/test.  Ignored for "
    "pd-ssd.",
)


_BOOT_DISK_SIZE_GB = flags.DEFINE_integer(
    "swap_encryption_boot_disk_size_gb",
    500,
    "Boot disk size in GiB for the benchmark nodepool.  500 GiB is "
    "required for the n4-highmem-32 + hyperdisk-balanced Config 2 run "
    "(see Engineer Assignments table in execution-plan.md).  "
    "For LSSD configs the boot disk is smaller; 100 GiB is fine.",
)


_ADD_SWAP_DISK = flags.DEFINE_boolean(
    "swap_encryption_add_swap_disk",
    False,
    "Attach a dedicated second disk to the benchmark nodepool for use as "
    "the swap device.  Required for dm-crypt measurement on single-boot-disk "
    "machines (n4-highmem-32, n4-highmem-8) because COS blocks device-mapper "
    "from pod namespaces.  The second disk is provisioned via "
    "--additional-node-disk using the same type/IOPS/throughput as the boot "
    "disk flags.",
)


_SWAP_DISK_SIZE_GB = flags.DEFINE_integer(
    "swap_encryption_swap_disk_size_gb",
    500,
    "Size in GiB of the dedicated swap disk when "
    "--swap_encryption_add_swap_disk is True.  Must satisfy the "
    "hyperdisk-balanced IOPS constraint: provisioned_iops ≤ size_gb × 80.",
)


_DS_NAME = "pkb-swap-benchmark"


_DS_NAMESPACE = "default"


_DS_LABEL = "pkb-swap-benchmark"


_active_pod: list[str] = []  # single-element list so closures can mutate it


_stress_vm_method: list[str] = (
    []
)  # single-element list; '' means no --vm-method flag


_degraded_reasons: list[str] = []


_pod_lost: list[str] = []


_oom_events: list[str] = []


_BENCHMARK_NODEPOOL = "benchmark"


_DEFAULT_NODEPOOL = "default-pool"


_FIO_JOBS = (
    ("rand_write_iops", "randwrite", "4k", 256, "Random write IOPS"),
    ("rand_read_iops", "randread", "4k", 256, "Random read IOPS"),
    ("rand_rw_mixed", "randrw", "4k", 256, "Mixed random R/W (50/50)"),
    ("seq_write_bw", "write", "1m", 64, "Sequential write bandwidth"),
    ("seq_read_bw", "read", "1m", 64, "Sequential read bandwidth"),
    ("lat_write", "randwrite", "4k", 1, "Random write latency"),
    ("lat_read", "randread", "4k", 1, "Random read latency"),
)


_VMSTAT_LOG = "/tmp/pkb_vmstat.log"


_PIDSTAT_LOG = "/tmp/pkb_pidstat.log"


_CRYPTO_PROCS = ("kswapd", "kworker", "kcryptd", "dmcrypt_write")


def _daemonset_yaml(image: str) -> str:
    """Render the privileged benchmark DaemonSet manifest.

    The manifest is a PKB data file rendered with Jinja2
    (data/cluster/swap_encryption_daemonset.yaml.j2) rather than an inline
    string, per PKB conventions.  The DaemonSet is pinned to the benchmark
    nodepool via nodeSelector so it never lands on the dummy default pool.
    """
    return vm_util.ReadAndRenderJinja2Template(
        "cluster/swap_encryption_daemonset.yaml.j2",
        ds_name=_DS_NAME,
        ds_namespace=_DS_NAMESPACE,
        ds_label=_DS_LABEL,
        benchmark_nodepool=_BENCHMARK_NODEPOOL,
        image=image,
        kernel_version=_KERNEL_VERSION.value,
    )


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


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
    cluster = spec.container_cluster

    # ── Step 2a: add real benchmark nodepool ────────────────────────────────
    if getattr(cluster, "project", None):
        # GCP path: true two-step nodepool setup
        logging.info("[swap_encryption] Step 2a: creating benchmark nodepool")
        _create_benchmark_node_pool(cluster)

        # ── Step 2b: wait for the benchmark node to join and be Ready ─────────
        logging.info("[swap_encryption] Step 2b: waiting for benchmark node")
        _wait_for_benchmark_node()

        # ── Step 2b2: attach dedicated swap disk (if requested) ───────────────
        # --additional-node-disk is not available in all gcloud versions, so we
        # create + attach the disk after the node is up using gcloud compute.
        if _ADD_SWAP_DISK.value:
            logging.info(
                "[swap_encryption] Step 2b2: attaching dedicated swap disk"
            )
            _attach_swap_disk(cluster)
    else:
        # AWS / EKS: nodepool management is external.  PKB's cluster creation
        # labels nodes pkb_nodepool=default, so re-label all existing nodes here
        # to match the DaemonSet nodeSelector (pkb_nodepool=benchmark).
        logging.info(
            "[swap_encryption] EKS cluster — labelling existing nodes with "
            "pkb_nodepool=%s so the DaemonSet nodeSelector matches.",
            _BENCHMARK_NODEPOOL,
        )
        kubectl.RunKubectlCommand([
            "label",
            "nodes",
            "--all",
            "--overwrite",
            f"pkb_nodepool={_BENCHMARK_NODEPOOL}",
        ])
        # io2 test-matrix row: create + attach a real io2 EBS volume so swap runs
        # on io2 hardware-encrypted storage (no-op unless swap_type=io2).
        _ensure_io2_volume()

    # ── Step 2c: deploy DaemonSet ────────────────────────────────────────────
    # Deploy and wait for the pod BEFORE deleting the default nodepool.
    # Deleting the default pool while the benchmark node is still joining causes
    # a temporary API server i/o timeout (control plane busy with two nodepool
    # ops simultaneously).  Once the pod is Running the cluster is fully stable.
    logging.info("[swap_encryption] Step 2c: deploying privileged DaemonSet")
    _deploy_daemonset()

    pod = _wait_for_benchmark_pod()
    logging.info("[swap_encryption] Benchmark pod ready: %s", pod)

    # ── Step 2d: now safe to remove the dummy default nodepool ───────────────
    if getattr(cluster, "project", None):
        logging.info(
            "[swap_encryption] Step 2d: deleting dummy default nodepool"
        )
        _delete_default_node_pool(cluster)
        # The DaemonSet pod may be evicted and rescheduled with a new name during
        # the nodepool deletion (cluster control plane briefly interrupts pod
        # lifecycle).  Re-resolve the pod name to avoid stale-reference errors on
        # all subsequent _pod_exec calls.
        logging.info(
            "[swap_encryption] Step 2d: re-resolving benchmark pod "
            "after nodepool deletion"
        )
        pod = _wait_for_benchmark_pod()
        logging.info("[swap_encryption] Benchmark pod (post-deletion): %s", pod)

    # Tune kernel swap aggressiveness.
    # vm.swappiness=100 (maximum): GKE nodes default to 0 (avoid swap, prefer
    # OOM-kill).  At 60 the kernel still under-swapped on n4-highmem-32 — under
    # cgroup-level memory pressure with ~160 GB node RAM free it would leave
    # anonymous pages resident and record swap_out ~0 (run bb4a782d), making the
    # result non-deterministic.  100 maximally biases the kernel toward paging
    # anonymous pages out to the (encrypted) swap device, which is exactly the
    # path this benchmark is meant to exercise.
    _pod_exec(pod, "sysctl -w vm.swappiness=100", ignore_failure=True)
    if _MIN_FREE_KBYTES.value > 0:
        _pod_exec(pod, f"sysctl -w vm.min_free_kbytes={_MIN_FREE_KBYTES.value}")

    # Unlock container cgroup swap.
    # GKE cgroup v2 sets memory.swap.max=0 per-container even when the node has
    # a swap device.  This blocks swap for the container regardless of
    # vm.swappiness.  Stress-ng gets OOM-killed in ~15s because the kernel can
    # page out for this cgroup.  Set 'max' so the container can use all swap.
    _pod_exec(
        pod,
        textwrap.dedent("""
    PKB_CG=$(awk -F: '/^0::/{print $3; exit}' /proc/self/cgroup 2>/dev/null)
    if [ -n "$PKB_CG" ] && [ -f "/sys/fs/cgroup${PKB_CG}/memory.swap.max" ]; then
      echo max > "/sys/fs/cgroup${PKB_CG}/memory.swap.max" 2>/dev/null || true
    fi
    PKB_CG1=$(awk -F: '/:memory:/{print $3; exit}' /proc/self/cgroup 2>/dev/null)
    if [ -n "$PKB_CG1" ] && \
       [ -f "/sys/fs/cgroup/memory${PKB_CG1}/memory.memsw.limit_in_bytes" ]; then
      echo -1 > "/sys/fs/cgroup/memory${PKB_CG1}/memory.memsw.limit_in_bytes" \
        2>/dev/null || true
    fi
  """),
        ignore_failure=True,
    )

    # Enable zswap if requested
    if _ENABLE_ZSWAP.value:
        _enable_zswap(pod)

    # Configure cloud-specific swap
    cloud = _detect_cloud(pod)
    logging.info("[swap_encryption] Detected cloud: %s", cloud)

    if cloud == "gcp":
        _setup_gke_swap(pod)
    elif cloud == "aws":
        _setup_eks_swap(pod)
    else:
        logging.warning(
            "[swap_encryption] Unknown cloud – falling back to plain swapfile"
        )
        _setup_plain_swap_file(pod, _SWAP_SIZE_GB.value)


def _phase_selected(token: str) -> bool:
    """Return True if phase `token` should run given --swap_encryption_phases.

    'all' (the default) selects every phase.  Otherwise only the comma-separated
    tokens listed in the flag run.  Tokens: fio, 2a, 2b, 3a, 3b, 3c.
    """
    selected = [p.strip().lower() for p in _PHASES.value if p.strip()]
    return (not selected) or ("all" in selected) or (token.lower() in selected)


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
    pod = _wait_for_benchmark_pod()
    # Initialise the module-level active-pod tracker so _pod_exec and
    # _recover_pod can transparently redirect to a replacement pod if the
    # original is evicted during the run.
    _active_pod.clear()
    _active_pod.append(pod)
    _degraded_reasons.clear()
    _pod_lost.clear()
    _oom_events.clear()
    original_pod = pod
    swap_dev = _detect_swap_device(pod)
    base_meta = _build_metadata(pod, swap_dev)
    results: list[sample.Sample] = []
    t_run_start = time.time()

    logging.info("[swap_encryption] swap device: %s", swap_dev)

    # ── Tier 1 / Gate 1: fio microbenchmarks ─────────────────────────────────
    tier1_results = []
    if _phase_selected("fio"):
        logging.info(
            "[swap_encryption] ── Tier 1 / Gate 1: fio microbenchmarks ──"
        )
        try:
            tier1_results = _phase1_fio(pod, swap_dev, base_meta)
            results += tier1_results
        except Exception as e:  # pylint: disable=broad-except
            logging.error(
                "[swap_encryption] Gate 1 FAILED — fio phase error: %s", e
            )
            logging.error(
                "[swap_encryption] Skipping Tiers 2 and 3 (no swap device)"
            )
            return results

        if not tier1_results:
            logging.warning(
                "[swap_encryption] Gate 1 produced no samples "
                "(loop-device skip or parse error) — "
                "continuing to Tier 2 with caution"
            )
    else:
        logging.info(
            "[swap_encryption] Skipping Tier 1 (fio) — not selected by "
            "--swap_encryption_phases=%s",
            ",".join(_PHASES.value),
        )

    # ── Tier 2 / Gate 2: stress-ng CPU overhead + I/O interference ───────────
    if _phase_selected("2a") or _phase_selected("2b"):
        logging.info(
            "[swap_encryption] ── Tier 2 / Gate 2: stress-ng phases ──"
        )
        try:
            if _phase_selected("2a"):
                logging.info("[swap_encryption] Phase 2a: CPU overhead")
                results += _phase2a_cpu_overhead(pod, base_meta)
            if _phase_selected("2b"):
                logging.info("[swap_encryption] Phase 2b: I/O interference")
                results += _phase2b_io_interference(pod, base_meta)
        except Exception as e:  # pylint: disable=broad-except
            logging.error(
                "[swap_encryption] Gate 2 FAILED — stress phase error: %s", e
            )
            logging.warning(
                "[swap_encryption] Proceeding to Tier 3 (workloads are "
                "independent of stress-ng results)"
            )

    # ── Cost estimate ─────────────────────────────────────────────────────────
    if _COLLECT_COST.value:
        elapsed = time.time() - t_run_start
        results += _collect_cost_sample(pod, elapsed, base_meta)

    # ── Final degradation gate ────────────────────────────────────────────────
    # The phase try/except blocks above keep the run alive so partial data is
    # still collected, but that means a catastrophic failure (pod OOM-evicted
    # mid-run, no fio data, stress-ng killed before it could drive swap I/O)
    # would otherwise be reported by PKB as SUCCEEDED with empty/garbage metrics.
    # Detect those conditions here and surface them explicitly.
    if _active_pod and _active_pod[0] != original_pod:
        _degraded_reasons.append(
            f"benchmark pod was replaced during the run ({original_pod} →"
            f" {_active_pod[0]}) — it was OOM-evicted under swap pressure;"
            " phases executed after the eviction ran against a"
            " freshly-initialised pod (empty /tmp, swap re-setup) and may be"
            " invalid"
        )
    if _pod_lost:
        _degraded_reasons.append(
            "benchmark pod(s) went NotFound during the run"
            f' ({", ".join(_pod_lost)}) — the pod died (node memory-pressure'
            " eviction or container exit) and any phase running at or after"
            " that point (e.g. kernel-build baseline, OpenSearch) produced"
            " invalid data"
        )
    if _oom_events:
        _degraded_reasons.append(
            f"OOM kill(s) (rc=137) occurred during the run on pod(s) "
            f'{", ".join(_oom_events)} — a phase exceeded memory and was'
            " killed by "
            f"the OOM killer (the container may have restarted in place), so"
            f" the "
            f"affected phase(s) produced no or partial data"
        )

    if _phase_selected("fio") and not tier1_results:
        if swap_dev.startswith("/dev/loop"):
            # Expected: COS blocks device-mapper from pod namespaces on single-disk
            # nodes (n2/n4 without --swap_encryption_add_swap_disk or lssd).
            # Tier 2/3 results are still valid; do NOT mark the run as degraded.
            logging.warning(
                "[swap_encryption] Gate 1 (fio) skipped — loop device %s has no"
                " dm-crypt support from inside a pod.  Tier 2/3 results are"
                " valid. Use c4-*-lssd or --swap_encryption_add_swap_disk for"
                " fio data.",
                swap_dev,
            )
        else:
            _degraded_reasons.append(
                "Gate 1 (fio microbenchmarks) produced no samples — the raw"
                " swap device was never characterised"
            )

    degraded = bool(_degraded_reasons)
    results.append(
        sample.Sample(
            "swap_encryption_run_status",
            0.0 if degraded else 1.0,
            "status",
            dict(
                base_meta,
                degraded=degraded,
                degraded_reasons="; ".join(_degraded_reasons) or "none",
                num_samples=len(results) + 1,
            ),
        )
    )

    if degraded:
        msg = "[swap_encryption] RUN DEGRADED — " + "; ".join(_degraded_reasons)
        logging.error(msg)
        if _FAIL_ON_DEGRADED.value:
            # Raise so PKB marks the benchmark FAILED instead of SUCCEEDED.  The
            # samples collected so far are still published by PKB before the failure
            # is recorded, so no data is lost.
            raise errors.Benchmarks.RunError(msg)
    else:
        logging.info(
            "[swap_encryption] Run completed cleanly (%d samples)", len(results)
        )

    return results


def Cleanup(spec) -> None:
    """Remove the DaemonSet and tear down any swap configuration."""
    pod = _wait_for_benchmark_pod(timeout=30)
    if pod:
        _pod_exec(pod, "swapoff -a 2>/dev/null || true", ignore_failure=True)
        _pod_exec(
            pod,
            textwrap.dedent("""
      swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
      dmsetup remove --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    """),
            ignore_failure=True,
        )
        # Clean up loop device backing files (single-disk fallback path).
        _pod_exec(
            pod,
            textwrap.dedent("""
      for backing in /var/pkb_swap_backing /run/pkb_swap_backing \
                     /mnt/stateful_partition/pkb_swap_backing
      do
        losetup -j "$backing" 2>/dev/null | awk -F: '{print $1}' | \
          while read dev
          do
            losetup -d "$dev" 2>/dev/null || true
          done
        rm -f "$backing"
      done
    """),
            ignore_failure=True,
        )
        _pod_exec(
            pod,
            "pkill -9 'stress-ng|fio' 2>/dev/null || true",
            ignore_failure=True,
        )

    _delete_daemonset()

    # Detach and delete the dedicated swap disk if one was provisioned.
    cluster = spec.container_cluster
    if _ADD_SWAP_DISK.value and getattr(cluster, "project", None):
        _detach_and_delete_swap_disk(cluster)


def _configure_eks_kubelet_swap(spec) -> None:
    """Configure EKS kubelet for LimitedSwap via nodeadm bootstrap.

    NOTE: Deferred — requires Ajay's PR #6780 (SwapConfigSpec + nodeadm
    integration) to merge.  When that lands, EKS node pools should include
    a preBootstrapCommands block writing nodeadm config with
    memorySwapBehavior: LimitedSwap before kubelet starts.

    See also: https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/pull/6780
    """
    logging.warning(
        "[swap_encryption] EKS kubelet LimitedSwap config via nodeadm is "
        "deferred (blocked on PR #6780 — SwapConfigSpec). "
        "EKS nodes will use default kubelet swap settings until that PR merges."
    )


def _deploy_daemonset() -> None:
    """Apply the benchmark DaemonSet manifest to the cluster."""
    manifest = _daemonset_yaml(image=_DAEMONSET_IMAGE.value)
    with vm_util.NamedTemporaryFile(mode="w", suffix=".yaml") as f:
        f.write(manifest)
        f.close()
        kubectl.RunKubectlCommand(["apply", "-f", f.name])
    logging.info("[swap_encryption] DaemonSet applied")


def _wait_for_benchmark_pod(timeout: int = 900) -> str | None:
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
                                _log_pod_events(pod_name)
            else:
                logging.info(
                    "[swap_encryption] Waiting for DaemonSet pod to appear..."
                )

        # ── Step 2: poll for /tmp/pkb_ready sentinel ────────────────────────────
        if ready_pod is not None:
            sentinel_out, sentinel_err, sentinel_rc = kubectl.RunKubectlCommand(
                [
                    "exec",
                    ready_pod,
                    "-n",
                    _DS_NAMESPACE,
                    "--",
                    "test",
                    "-f",
                    "/tmp/pkb_ready",
                ],
                raise_on_failure=False,
            )
            if sentinel_rc == 0:
                logging.info(
                    "[swap_encryption] Pod %s ready (tools installed)",
                    ready_pod,
                )
                return ready_pod
            # "container not found" means the container crashed (CrashLoopBackOff or
            # exited) — treat it as a hard reset: re-check pod phase on next iteration.
            if (
                "container not found" in sentinel_err
                or "unable to upgrade connection" in sentinel_err
            ):
                logging.warning(
                    "[swap_encryption] Pod %s: container not running (%s) "
                    "— will re-check pod state",
                    ready_pod,
                    sentinel_err.strip(),
                )
                ready_pod = None
                last_phase = ""
            else:
                logging.info(
                    "[swap_encryption] Pod %s: still installing tools...",
                    ready_pod,
                )

        time.sleep(15)

    logging.warning(
        "[swap_encryption] Benchmark pod not ready after %ds", timeout
    )
    return None


def _log_pod_events(pod_name: str) -> None:
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


def _delete_daemonset() -> None:
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


def _build_node_startup_script(enable_dmcrypt: bool, lssd: bool) -> str:
    """Return a bash startup script for the benchmark nodepool.

    NOTE: This function is not currently used. GKE reserves the
    `startup-script` node metadata key, so dm-crypt setup is performed
    from within the privileged DaemonSet pod instead (see
    _setup_gke_hyperdisk_swap / _setup_gke_lssd_swap). Kept as reference.

    Args:
      enable_dmcrypt: When True, wrap the swap device in dm-crypt plain
        mode (aes-xts-plain64, ephemeral random key) matching GKE's
        go/node:swap-encryption implementation.
      lssd: When True, build a RAID-0 array across all local SSDs before
        setting up swap (matches go/gke-swap-lssd).

    Returns:
      A bash script string suitable for running as root at node boot.
    """
    dmcrypt_str = "true" if enable_dmcrypt else "false"
    lssd_str = "true" if lssd else "false"

    return textwrap.dedent(f"""\
    #!/bin/bash
    # PKB swap_encryption_benchmark — nodepool startup script.
    # Configures swap once at node boot so all benchmark phases see a
    # pre-warmed swap device.  Runs as root on the COS host.
    set -euo pipefail
    ENABLE_DMCRYPT={dmcrypt_str}
    LSSD={lssd_str}

    _wait_dev() {{
      local d=$1 i
      for i in $(seq 1 30); do [ -b "$d" ] && return 0; sleep 2; done
      echo "[pkb-startup] device $d not ready" >&2; return 1
    }}

    _boot_dev() {{
      lsblk -no pkname "$(findmnt -n -o SOURCE /)" 2>/dev/null | head -1 || echo nvme0n1
    }}

    if $LSSD; then
      BOOT=$(_boot_dev)
      # Collect all non-rotational non-boot block devices (local SSDs)
      DEVS=$(lsblk -d -o NAME,ROTA | awk '$2=="0"{{print "/dev/"$1}}' | grep -v "/dev/$BOOT" || true)
      N=$(echo "$DEVS" | grep -c /dev/ || true)
      if [ "$N" -gt 1 ]; then
        modprobe raid0 || true
        # shellcheck disable=SC2086
        mdadm --create /dev/md0 --level=0 --raid-devices="$N" $DEVS --force
        TARGET=/dev/md0
      elif [ "$N" -eq 1 ]; then
        TARGET=$(echo "$DEVS" | head -1)
      else
        echo "[pkb-startup] no LSSD devices found; skipping swap setup" >&2
        exit 0
      fi
    else
      BOOT=$(_boot_dev)
      RAW=$(lsblk -d -o NAME,TYPE | awk '$2=="disk"{{print $1}}' | grep -v "^$BOOT$" | head -1 || true)
      if [ -z "$RAW" ]; then
        echo "[pkb-startup] no secondary disk found for hyperdisk swap" >&2
        exit 0
      fi
      TARGET=/dev/$RAW
    fi

    _wait_dev "$TARGET"

    if $ENABLE_DMCRYPT; then
      modprobe dm-crypt || true
      dd if=/dev/urandom bs=32 count=1 2>/dev/null | \\
        cryptsetup open --type plain \\
          --cipher aes-xts-plain64 --key-size 256 \\
          --key-file=- "$TARGET" pkb_swap
      SWAP_DEV=/dev/mapper/pkb_swap
    else
      SWAP_DEV=$TARGET
    fi

    mkswap "$SWAP_DEV"
    swapon "$SWAP_DEV"
    echo "[pkb-startup] swap active on $SWAP_DEV (dmcrypt=$ENABLE_DMCRYPT lssd=$LSSD)"
  """)


_HYPERDISK_MAX_IOPS_PER_MBPS = (
    256  # GCP Hyperdisk Balanced: IOPS <= 256 x MiB/s
)


def _valid_hyperdisk_throughput(iops: int, throughput: int) -> int:
    """Return a throughput (MiB/s) that satisfies GCP's Hyperdisk constraint.

    Hyperdisk Balanced rejects disk creation when provisioned IOPS exceed
    256 x provisioned throughput (MiB/s) — e.g. 80000 IOPS with 300 MiB/s fails
    with "Requested provisioned throughput is too low for the provisioned iops".
    Clamp throughput UP to the minimum the requested IOPS need (plus a small
    margin) and warn, so a mismatched flag pairing cannot abort node-pool/disk
    creation.
    """
    min_tput = -(-int(iops) // _HYPERDISK_MAX_IOPS_PER_MBPS)  # ceil(iops/256)
    if throughput < min_tput:
        logging.warning(
            "[swap_encryption] boot/swap disk throughput %d MiB/s is too low"
            " for %d IOPS (Hyperdisk needs >= ceil(iops/256) = %d MiB/s);"
            " raising to %d",
            throughput,
            iops,
            min_tput,
            min_tput,
        )
        return min_tput
    return throughput


def _create_benchmark_node_pool(cluster) -> None:
    """Add the benchmark nodepool to the existing cluster (Step 2 of setup).

    Uses:
      --swap_encryption_benchmark_machine_type  (default n4-highmem-32)
      --swap_encryption_node_image_type         (default COS_CONTAINERD)
      --swap_encryption_boot_disk_iops          (default 80000)
      --swap_encryption_enable_dmcrypt          (default True)

    The nodepool is labelled pkb_nodepool=benchmark so the DaemonSet
    nodeSelector targets it exclusively.  dm-crypt swap setup is performed
    from within the privileged DaemonSet pod (see _setup_gke_hyperdisk_swap /
    _setup_gke_lssd_swap) — we do NOT inject a startup-script via node metadata
    because GKE reserves that metadata key and rejects it at the API level.
    """
    machine_type = _BENCHMARK_MACHINE_TYPE.value
    # Auto-detect LSSD from machine type name; flag overrides only when True.
    is_lssd = _BENCHMARK_LSSD.value or "lssd" in machine_type.lower()

    # Determine zone/region from the cluster object.
    zone_flags: list[str] = []
    if getattr(cluster, "zones", None):
        zone_flags = ["--zone", cluster.zones[0]]
    elif getattr(cluster, "region", None):
        zone_flags = ["--region", cluster.region]

    # LSSD configs only need a small boot disk (OS only; swap is on local NVMe).
    # Hyperdisk configs need 500 GiB to hit 80 000 IOPS (the IOPS/GiB ratio on
    # hyperdisk-balanced is 1:1 up to the provisioned ceiling, so a 100 GiB disk
    # can only provision up to 100 000 IOPS but a 500 GiB gives comfortable
    # headroom and matches the Config 2 spec in the Engineer Assignments table).
    disk_size_gb = 100 if is_lssd else _BOOT_DISK_SIZE_GB.value

    disk_type = _BOOT_DISK_TYPE.value
    cmd = [
        "gcloud",
        "container",
        "node-pools",
        "create",
        _BENCHMARK_NODEPOOL,
        "--cluster",
        cluster.name,
        "--project",
        cluster.project,
        "--machine-type",
        machine_type,
        "--image-type",
        _NODE_IMAGE_TYPE.value,
        "--disk-type",
        disk_type,
        "--disk-size",
        str(disk_size_gb),
        "--num-nodes",
        "1",
        "--node-labels",
        f"pkb_nodepool={_BENCHMARK_NODEPOOL}",
        "--no-enable-autoupgrade",
        "--no-enable-autorepair",
    ] + zone_flags

    # IOPS and throughput provisioning only applies to hyperdisk-* types AND
    # only when the boot disk is also the swap device (non-LSSD configs).
    # For LSSD machines the boot disk is OS-only; swap is on local NVMe.
    # Provisioning 80k IOPS on a 100 GiB boot disk would exceed the
    # hyperdisk-balanced per-GiB cap (80 IOPS/GiB × 100 GiB = 8 000 max).
    if disk_type.startswith("hyperdisk") and not is_lssd:
        cmd += [
            "--boot-disk-provisioned-iops",
            str(_BOOT_DISK_IOPS.value),
            "--boot-disk-provisioned-throughput",
            str(
                _valid_hyperdisk_throughput(
                    _BOOT_DISK_IOPS.value, _BOOT_DISK_THROUGHPUT.value
                )
            ),
        ]

    # For LSSD machines, expose local NVMe as raw block devices so fio/mdadm
    # can access them directly (go/gke-swap-lssd uses local-nvme-ssd-block).
    if is_lssd:
        cmd += ["--local-nvme-ssd-block", f"count={_LSSD_COUNT.value}"]

    logging.info(
        "[swap_encryption] Creating benchmark nodepool: %s / %s / "
        "image=%s / disk=%dGiB / iops=%d / dmcrypt=%s / lssd=%s / "
        "add_swap_disk=%s",
        _BENCHMARK_NODEPOOL,
        machine_type,
        _NODE_IMAGE_TYPE.value,
        disk_size_gb,
        _BOOT_DISK_IOPS.value,
        _ENABLE_DMCRYPT.value,
        is_lssd,
        _ADD_SWAP_DISK.value,
    )

    # LSSD nodepools take longer to provision than PD-only nodepools because
    # GKE must also initialise the local NVMe devices before marking nodes Ready.
    # 1200 s (20 min) covers observed worst-case times on c4-lssd and n4 configs.
    stdout, stderr, rc = vm_util.IssueCommand(
        cmd, timeout=1200, raise_on_failure=False
    )

    if rc != 0:
        # Idempotent prepare: if the nodepool already exists (e.g. re-running
        # --run_stage=prepare,run to redeploy the DaemonSet onto an existing
        # cluster), reuse it instead of failing.  gcloud returns a 409 /
        # "Already exists" message in this case.
        low = (stderr or "").lower()
        if (
            "already exists" in low
            or "alreadyexists" in low
            or "code=409" in low
        ):
            logging.info(
                "[swap_encryption] Benchmark nodepool already exists — "
                "reusing it (idempotent prepare); proceeding to DaemonSet"
            )
            return
        raise errors.Benchmarks.RunError(
            "[swap_encryption] Failed to create benchmark nodepool "
            f"(rc={rc}): {stderr}"
        )
    logging.info("[swap_encryption] Benchmark nodepool ready")


def _wait_for_benchmark_node(timeout: int = 900) -> None:
    """Block until a node labelled pkb_nodepool=benchmark is Ready.

    gcloud container node-pools create returns as soon as the API accepts the
    request — the actual node VM may take another 2-4 minutes to boot, join the
    cluster, and pass its readiness checks.  Deploying the DaemonSet before that
    point leaves the pod Pending indefinitely because the nodeSelector finds no
    eligible node.

    This function polls kubectl every 15 s until at least one node with
    pkb_nodepool=benchmark has Ready=True, then returns.
    """
    deadline = time.time() + timeout
    logging.info(
        "[swap_encryption] Waiting for benchmark node "
        "(pkb_nodepool=benchmark) to be Ready..."
    )
    while time.time() < deadline:
        out, _, rc = kubectl.RunKubectlCommand(
            [
                "get",
                "nodes",
                "-l",
                f"pkb_nodepool={_BENCHMARK_NODEPOOL}",
                "-o",
                r"jsonpath={range .items[*]}"
                r'{.metadata.name}{"\t"}'
                r'{range .status.conditions[?(@.type=="Ready")]}'
                r'{.status}{"\n"}{end}{end}',
            ],
            raise_on_failure=False,
        )

        if rc == 0 and out.strip():
            for line in out.strip().splitlines():
                parts = line.split("\t")
                if len(parts) == 2 and parts[1].strip() == "True":
                    logging.info(
                        "[swap_encryption] Benchmark node ready: %s",
                        parts[0].strip(),
                    )
                    return

        logging.info(
            "[swap_encryption] Benchmark node not yet Ready — "
            "retrying in 15 s..."
        )
        time.sleep(15)

    raise errors.Benchmarks.RunError(
        "[swap_encryption] Timed out waiting for benchmark node "
        f"(pkb_nodepool={_BENCHMARK_NODEPOOL}) to become Ready "
        f"after {timeout}s"
    )


def _attach_swap_disk(cluster) -> None:
    """Create a dedicated hyperdisk and attach it to the benchmark node.

    gcloud container node-pools create --additional-node-disk is not available
    in all gcloud SDK versions, so we use gcloud compute to create the disk and
    attach it after the node is ready.  In GKE the Kubernetes node name is the
    same as the GCE instance name, so no translation is needed.

    After attachment the disk appears as /dev/sdb (or /dev/nvme1n1 on NVMe
    nodes) inside the pod, and _setup_gke_hyperdisk_swap detects it via lsblk.

    The disk is named pkb-swap-<cluster-name> to avoid name collisions across
    concurrent runs.  Cleanup deletes it in Cleanup() if it exists.
    """
    # Resolve zone from cluster
    zone = None
    if getattr(cluster, "zones", None):
        zone = cluster.zones[0]
    elif getattr(cluster, "region", None):
        zone = cluster.region
    if not zone:
        raise errors.Benchmarks.RunError(
            "[swap_encryption] Cannot attach swap disk: cluster zone unknown"
        )

    project = cluster.project
    disk_name = f"pkb-swap-{cluster.name}"
    disk_type = _BOOT_DISK_TYPE.value
    disk_size_gb = _SWAP_DISK_SIZE_GB.value

    # ── Step 1: get the GCE instance name of the benchmark node ───────────────
    node_out, _, rc = kubectl.RunKubectlCommand(
        [
            "get",
            "nodes",
            "-l",
            f"pkb_nodepool={_BENCHMARK_NODEPOOL}",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
        raise_on_failure=False,
    )
    instance_name = node_out.strip()
    if rc != 0 or not instance_name:
        raise errors.Benchmarks.RunError(
            "[swap_encryption] Cannot find benchmark node for swap disk attach"
        )
    logging.info("[swap_encryption] Benchmark node instance: %s", instance_name)

    # ── Step 2: create the hyperdisk ──────────────────────────────────────────
    logging.info(
        "[swap_encryption] Creating swap disk %s (%dGiB %s)",
        disk_name,
        disk_size_gb,
        disk_type,
    )
    create_cmd = [
        "gcloud",
        "compute",
        "disks",
        "create",
        disk_name,
        "--project",
        project,
        "--zone",
        zone,
        "--type",
        disk_type,
        "--size",
        f"{disk_size_gb}GB",
        "--quiet",
    ]
    if disk_type.startswith("hyperdisk"):
        create_cmd += [
            "--provisioned-iops",
            str(_BOOT_DISK_IOPS.value),
            "--provisioned-throughput",
            str(
                _valid_hyperdisk_throughput(
                    _BOOT_DISK_IOPS.value, _BOOT_DISK_THROUGHPUT.value
                )
            ),
        ]
    _, stderr, rc = vm_util.IssueCommand(
        create_cmd, timeout=120, raise_on_failure=False
    )
    if rc != 0:
        raise errors.Benchmarks.RunError(
            f"[swap_encryption] Failed to create swap disk {disk_name}:"
            f" {stderr}"
        )

    # ── Step 3: attach the disk to the node VM ────────────────────────────────
    logging.info(
        "[swap_encryption] Attaching swap disk %s to %s",
        disk_name,
        instance_name,
    )
    attach_cmd = [
        "gcloud",
        "compute",
        "instances",
        "attach-disk",
        instance_name,
        "--project",
        project,
        "--zone",
        zone,
        "--disk",
        disk_name,
        "--device-name",
        "pkb-swap",
        "--quiet",
    ]
    _, stderr, rc = vm_util.IssueCommand(
        attach_cmd, timeout=120, raise_on_failure=False
    )
    if rc != 0:
        raise errors.Benchmarks.RunError(
            f"[swap_encryption] Failed to attach swap disk to {instance_name}: "
            f"{stderr}"
        )
    logging.info(
        "[swap_encryption] Swap disk attached: %s → %s",
        disk_name,
        instance_name,
    )


def _delete_disk_by_name(disk_name: str, project: str, zone: str) -> bool:
    """Detach (if attached) and delete a GCE disk, robustly, with retries.

    Finds the attached instance from the disk's own `users` field rather than
    kubectl — kubectl is often unavailable during teardown (cluster being
    deleted), which previously left the disk attached and undeletable, so it
    leaked.  Returns True if the disk is gone (deleted or already absent).
    """
    for attempt in range(1, 5):
        users, _, rc = vm_util.IssueCommand(
            [
                "gcloud",
                "compute",
                "disks",
                "describe",
                disk_name,
                "--project",
                project,
                "--zone",
                zone,
                "--format=value(users)",
            ],
            timeout=60,
            raise_on_failure=False,
        )
        if rc != 0:
            logging.info(
                "[swap_encryption] Swap disk %s not present — nothing to "
                "delete",
                disk_name,
            )
            return True  # already gone
        user = users.strip()
        if user:
            inst = user.split("/")[-1]
            logging.info(
                "[swap_encryption] Detaching swap disk %s from %s",
                disk_name,
                inst,
            )
            vm_util.IssueCommand(
                [
                    "gcloud",
                    "compute",
                    "instances",
                    "detach-disk",
                    inst,
                    "--project",
                    project,
                    "--zone",
                    zone,
                    "--disk",
                    disk_name,
                    "--quiet",
                ],
                timeout=120,
                raise_on_failure=False,
            )
        _, derr, drc = vm_util.IssueCommand(
            [
                "gcloud",
                "compute",
                "disks",
                "delete",
                disk_name,
                "--project",
                project,
                "--zone",
                zone,
                "--quiet",
            ],
            timeout=180,
            raise_on_failure=False,
        )
        if drc == 0:
            logging.info("[swap_encryption] Swap disk deleted: %s", disk_name)
            return True
        logging.warning(
            "[swap_encryption] Swap disk delete attempt %d/4 failed "
            "(%s); retrying in 10s",
            attempt,
            derr.strip()[:160],
        )
        time.sleep(10)
    logging.error(
        "[swap_encryption] Could NOT delete swap disk %s after retries "
        "— delete it manually: gcloud compute disks delete %s "
        "--zone %s --quiet",
        disk_name,
        disk_name,
        zone,
    )
    return False


def _detach_and_delete_swap_disk(cluster) -> None:
    """Detach and delete the dedicated swap disk created by _attach_swap_disk."""
    zone = None
    if getattr(cluster, "zones", None):
        zone = cluster.zones[0]
    elif getattr(cluster, "region", None):
        zone = cluster.region
    if not zone or not getattr(cluster, "project", None):
        return
    _delete_disk_by_name(f"pkb-swap-{cluster.name}", cluster.project, zone)


def _delete_default_node_pool(cluster) -> None:
    """Delete the dummy default nodepool after the benchmark pool is ready.

    The default nodepool (e2-medium) was only needed to satisfy GKE's
    requirement that a cluster must have at least one nodepool at creation time.
    Removing it stops the clock on its cost immediately.
    """
    zone_flags: list[str] = []
    if getattr(cluster, "zones", None):
        zone_flags = ["--zone", cluster.zones[0]]
    elif getattr(cluster, "region", None):
        zone_flags = ["--region", cluster.region]

    cmd = [
        "gcloud",
        "container",
        "node-pools",
        "delete",
        _DEFAULT_NODEPOOL,
        "--cluster",
        cluster.name,
        "--project",
        cluster.project,
        "--quiet",
    ] + zone_flags

    logging.info(
        "[swap_encryption] Deleting default nodepool: %s", _DEFAULT_NODEPOOL
    )
    stdout, stderr, rc = vm_util.IssueCommand(
        cmd, timeout=300, raise_on_failure=False
    )
    if rc != 0:
        logging.warning(
            "[swap_encryption] Could not delete default nodepool (rc=%d): %s",
            rc,
            stderr,
        )
    else:
        logging.info("[swap_encryption] Default nodepool deleted")


def _is_pod_gone(pod: str) -> bool:
    """Return True if the named pod no longer exists in the cluster.

    Used to distinguish OOM-killed container processes (pod still alive, rc=137)
    from OOM-evicted pods (pod gone, DaemonSet will create a replacement).
    """
    try:
        _, err, rc = kubectl.RunKubectlCommand(
            [
                "get",
                "pod",
                pod,
                "-n",
                _DS_NAMESPACE,
                "-o",
                "jsonpath={.metadata.name}",
            ],
            raise_on_failure=False,
            timeout=15,
        )
        return rc != 0 and "not found" in (err or "").lower()
    except Exception:  # pylint: disable=broad-except
        return False


def _pod_exec(
    pod: str,
    cmd: str,
    ignore_failure: bool = False,
    timeout: int = 300,
    _retries: int = 2,
) -> tuple[str, str]:
    """Run a shell command inside the benchmark pod via kubectl exec.

    Args:
      pod: Pod name returned by _wait_for_benchmark_pod.
      cmd: Shell command string passed to bash -c.
      ignore_failure: When True, non-zero exit codes are logged but not
        raised.
      timeout: Seconds before PKB kills the kubectl exec process. Default
        300 s matches PKB's IssueCommand default. Pass a larger value for
        long-running jobs (fio, stress-ng, kernel build).
      _retries: Number of automatic retries on transient GKE websocket
        resets ("connection reset by peer").  Set to 0 to disable retries
        for idempotent-sensitive commands.

    Returns:
      Tuple of (stdout, stderr) strings.
    """
    _TRANSIENT_ERRORS = ("connection reset by peer", "websocket: close")
    # Errors that indicate the container/pod is gone and needs recovery.
    # 'not found' covers "Error from server (NotFound): pods ... not found"
    # which occurs when the DaemonSet pod was evicted and recreated under a
    # new name (e.g. after OOM-triggered node pressure eviction).
    # 'deleted state' covers "cannot exec in a deleted state" — the container
    # was OOM-killed and is mid-termination (not yet recreated).
    _CONTAINER_GONE_ERRORS = (
        "container not found",
        "procReady not received",
        "unable to upgrade connection",
        "not found",
        "deleted state",
    )
    # Use the globally-tracked active pod name — it may have been updated by
    # a previous _recover_pod call when eviction replaced the pod.
    active = _active_pod[0] if _active_pod else pod

    for attempt in range(_retries + 1):
        out, err, rc = kubectl.RunKubectlCommand(
            ["exec", active, "-n", _DS_NAMESPACE, "--", "bash", "-c", cmd],
            raise_on_failure=False,
            raise_on_timeout=False,  # let _pod_exec's own retry loop handle transient resets
            timeout=timeout,
        )
        is_transient = rc != 0 and any(e in err for e in _TRANSIENT_ERRORS)
        if is_transient and attempt < _retries:
            logging.warning(
                "[swap_encryption] kubectl exec connection reset (attempt"
                " %d/%d); retrying in 10 s",
                attempt + 1,
                _retries + 1,
            )
            time.sleep(10)
            continue
        # rc=137 (SIGKILL): the OOM killer terminated the container process.
        # Two sub-cases:
        #   A) Pod eviction: pod is gone, DaemonSet recreates it under a new name.
        #   B) Container OOM restart: pod still exists, container restarts in place.
        #      (DaemonSet restartPolicy=Always restarts the container, /tmp is lost,
        #      tools must be re-installed before subsequent commands can run.)
        # In both cases we call _recover_pod to wait for tools + sentinel, and
        # we do NOT retry the OOM-triggering command itself.
        if rc == 137:
            # Record the OOM so the run-level gate can flag it even if the container
            # restarts in place under the same pod name (which leaves both the
            # "pod replaced" and "pod NotFound" checks silent).
            if active not in _oom_events:
                _oom_events.append(active)
            # CRITICAL: sleep before checking pod state.  Kubernetes takes a few
            # seconds to mark a just-evicted pod as Terminating / NotFound.  Without
            # this delay _recover_pod sees the pod still in "Running" phase, returns
            # the old pod name immediately, and every subsequent command fails with
            # "Error from server (NotFound): pods … not found".
            logging.warning(
                "[swap_encryption] rc=137 — sleeping 15s for Kubernetes to"
                " update pod state before recovery check"
            )
            time.sleep(15)
            pod_gone = _is_pod_gone(active)
            if pod_gone:
                logging.warning(
                    "[swap_encryption] OOM-eviction detected (rc=137, pod gone)"
                    " — recovering pod name for subsequent commands (not"
                    " retrying this cmd)"
                )
            else:
                logging.warning(
                    "[swap_encryption] Container OOM-killed (rc=137, pod still"
                    " exists) — waiting for container restart and tool"
                    " re-install before continuing"
                )
            new_pod = _recover_pod(active)
            if new_pod != active:
                logging.info(
                    "[swap_encryption] Pod name updated: %s → %s",
                    active,
                    new_pod,
                )
                if _active_pod:
                    _active_pod[0] = new_pod
                active = new_pod
            break  # Do NOT retry — the OOM cmd itself is not re-run on the new pod.

        is_container_gone = rc != 0 and any(
            e in err.lower() for e in _CONTAINER_GONE_ERRORS
        )
        if is_container_gone:
            # Record the loss for the run-level degradation gate REGARDLESS of retry
            # budget or ignore_failure.  A "pods … not found" on a best-effort command
            # (kernel build, opensearch, cleanup of a dead pod) still means the pod
            # died; without this the gate stays blind because _active_pod is only
            # renamed on the retry path below, which _retries=0 callers never reach.
            if active and active not in _pod_lost:
                _pod_lost.append(active)
                logging.error(
                    "[swap_encryption] Benchmark pod %s is gone (%s) —"
                    " recording run as degraded",
                    active,
                    (err or "").strip()[:160],
                )
            if attempt < _retries:
                logging.warning(
                    "[swap_encryption] Container gone/restarting (attempt"
                    " %d/%d) — waiting for pod to recover...",
                    attempt + 1,
                    _retries + 1,
                )
                new_pod = _recover_pod(active)
                if new_pod != active:
                    logging.info(
                        "[swap_encryption] Pod name updated: %s → %s",
                        active,
                        new_pod,
                    )
                    if _active_pod:
                        _active_pod[0] = new_pod
                    active = new_pod
                continue
        break

    if rc != 0 and not ignore_failure:
        raise errors.VmUtil.IssueCommandError(
            f"[swap_encryption] _pod_exec failed (rc={rc}): {err}"
        )
    return out, err


def _recover_pod(pod: str, timeout_sec: int = 600) -> str:
    """Wait for a DaemonSet container to recover after OOM kill or eviction.

    Handles two scenarios:
    1. Container OOM restart: same pod name, container restarting in place.
       DaemonSet restartPolicy=Always brings it back under the same pod name.
    2. Pod eviction/deletion: the pod is gone entirely; the DaemonSet creates
       a new pod with a DIFFERENT name.  We detect this by checking whether
       the named pod still exists; if not, we search by the DaemonSet label
       selector for a Running pod.

    Returns the (possibly new) pod name once it is Running and ready.
    """
    deadline = time.time() + timeout_sec
    logging.info(
        "[swap_encryption] Waiting for pod %s to recover (up to %ds)...",
        pod,
        timeout_sec,
    )

    # Phase 1: wait for a Running pod — either the named one (container
    # restart) or a replacement pod found via label selector (eviction).
    #
    # IMPORTANT: we query BOTH status.phase AND metadata.deletionTimestamp in a
    # single call.  When a pod is evicted, Kubernetes first sets deletionTimestamp
    # (the pod is "Terminating") while status.phase may still read "Running" for
    # several seconds.  Checking only status.phase causes a false-positive: we
    # return the old pod name immediately and every subsequent command fails with
    # "Error from server (NotFound)".  Checking deletionTimestamp catches this.
    recovered_pod = pod
    while time.time() < deadline:
        # IMPORTANT: capture stderr — kubectl writes "not found" to stderr, not
        # stdout.  When the pod is gone, status_out is empty and the error text
        # lives entirely in status_err.  Discarding stderr (using _) means the
        # 'not found' check below never fires and we spin until deadline.
        status_out, status_err, status_rc = kubectl.RunKubectlCommand(
            [
                "get",
                "pod",
                pod,
                "-n",
                _DS_NAMESPACE,
                "-o",
                "jsonpath={.status.phase}|{.metadata.deletionTimestamp}",
            ],
            raise_on_failure=False,
            timeout=30,
        )
        # Parse "Running|" (no deletionTimestamp) vs "Running|2026-…" (terminating)
        fields = status_out.strip().split("|")
        phase = fields[0].strip() if fields else ""
        is_terminating = len(fields) > 1 and bool(fields[1].strip())

        # Pod is genuinely Running and NOT being deleted — recovery complete.
        if status_rc == 0 and phase == "Running" and not is_terminating:
            break

        # Pod no longer exists, OR it exists but is being terminated (Terminating
        # state or deletionTimestamp set) — look for a replacement pod by label.
        pod_gone_or_terminating = (
            status_rc != 0 and "not found" in (status_out + status_err).lower()
        ) or is_terminating
        if pod_gone_or_terminating:
            label_out, _, label_rc = kubectl.RunKubectlCommand(
                [
                    "get",
                    "pods",
                    "-n",
                    _DS_NAMESPACE,
                    "-l",
                    f"app={_DS_LABEL}",
                    "-o",
                    (
                        'jsonpath={range .items[?(@.status.phase=="Running")]}'
                        '{.metadata.name}{"\\n"}{end}'
                    ),
                ],
                raise_on_failure=False,
                timeout=30,
            )
            new_pods = [
                p.strip()
                for p in label_out.strip().splitlines()
                if p.strip() and p.strip() != pod
            ]  # exclude the dying pod
            if label_rc == 0 and new_pods:
                recovered_pod = new_pods[0]
                logging.info(
                    "[swap_encryption] Original pod %s gone/terminating; "
                    "found replacement %s",
                    pod,
                    recovered_pod,
                )
                break

        time.sleep(10)
    else:
        raise errors.VmUtil.IssueCommandError(
            f"[swap_encryption] No Running pod found (original: {pod}) "
            f"within {timeout_sec}s after OOM kill / eviction"
        )

    # Phase 2: wait for init script to finish (sentinel written last).
    while time.time() < deadline:
        ready_out, _, ready_rc = kubectl.RunKubectlCommand(
            [
                "exec",
                recovered_pod,
                "-n",
                _DS_NAMESPACE,
                "--",
                "bash",
                "-c",
                "test -f /tmp/pkb_ready && echo READY",
            ],
            raise_on_failure=False,
            timeout=30,
        )
        if ready_rc == 0 and "READY" in ready_out:
            logging.info(
                "[swap_encryption] Pod %s recovered and ready", recovered_pod
            )
            return recovered_pod
        time.sleep(15)

    raise errors.VmUtil.IssueCommandError(
        f"[swap_encryption] Pod {recovered_pod} did not become ready "
        f"within {timeout_sec}s after OOM kill / eviction"
    )


def _detect_cloud(pod: str) -> str:
    """Detect GCP vs AWS from DMI product info exposed via /sys hostPath mount.

    DMI is the most reliable in-container detection method because it reads
    directly from the host kernel's SMBIOS table via /sys (already mounted).
    It avoids HTTP metadata endpoint quoting issues and network timeouts.

    Falls back to metadata HTTP endpoints if DMI is inconclusive.
    """
    # Primary: DMI product name / vendor (available via /sys hostPath mount)
    dmi_out, _ = _pod_exec(
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
    gcp_out, _ = _pod_exec(
        pod,
        "curl -s -m 3 "
        "http://metadata.google.internal/computeMetadata/v1/instance/zone "
        '-H Metadata-Flavor:Google 2>/dev/null || echo ""',
        ignore_failure=True,
    )
    if gcp_out.strip():
        logging.info("[swap_encryption] Cloud detected via metadata: gcp")
        return "gcp"

    # Tertiary: AWS IMDS (IMDSv2 token-based; IMDSv1 is often disabled).
    aws_out, _ = _pod_exec(
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


def _setup_gke_swap(pod: str) -> None:
    """Configure dm-crypt swap on the GKE node, mirroring go/node:swap-encryption.

    GKE nodes use dm-crypt with an ephemeral random key so that swap contents
    are encrypted at rest without requiring persistent key management.
    We replicate this exactly using cryptsetup in plain mode (no LUKS header).
    """
    swap_type = _SWAP_TYPE.value
    if swap_type == "auto":
        # Check whether Local SSDs are present
        lssd_out, _ = _pod_exec(
            pod,
            "lsblk -d -o NAME,MODEL | grep -i 'local\\|nvme' | "
            "grep -v 'nvme0' | awk '{print $1}' | head -1",
            ignore_failure=True,
        )
        swap_type = "lssd" if lssd_out.strip() else "hyperdisk"

    if swap_type == "lssd":
        _setup_gke_lssd_swap(pod)
    elif swap_type == "boot_disk":
        _setup_gke_bootdisk_swap(pod)
    else:
        _setup_gke_hyperdisk_swap(pod)


def _setup_gke_hyperdisk_swap(pod: str) -> None:
    """Configure dm-crypt swap on hyperdisk-balanced (GKE default).

    Disk detection is split into two separate commands so that the boot-device
    name is resolved first and then substituted as a literal string — nested
    $() expansions inside a kubectl exec bash -c argument are unreliable.

    If no dedicated data disk is attached (single-disk node) dm-crypt is set up
    over a loop device backed by a file on the boot hyperdisk, which still
    exercises the full encryption path on the same storage tier.
    """
    logging.info("[swap_encryption] GKE: setting up dm-crypt on hyperdisk")

    # Step 1: identify the boot device name (e.g. "nvme0n1", "sda")
    boot_out, _ = _pod_exec(
        pod,
        'lsblk -no pkname "$(findmnt -n -o SOURCE /)" 2>/dev/null | head -1',
        ignore_failure=True,
    )
    boot_base = boot_out.strip() or "nvme0n1"
    logging.info("[swap_encryption] GKE: boot device: %s", boot_base)

    # Step 2: find a non-boot disk using the literal name from step 1
    disk_out, _ = _pod_exec(
        pod,
        "lsblk -d -o NAME,TYPE | awk '$2==\"disk\"{print $1}' "
        f"| grep -v '^{boot_base}$' | head -1",
        ignore_failure=True,
    )
    disk_name = disk_out.strip()

    if not disk_name:
        logging.info(
            "[swap_encryption] No dedicated data disk found – "
            "falling back to loop device on /mnt/stateful_partition "
            "(direct-io=on, dm-crypt=%s)",
            _ENABLE_DMCRYPT.value,
        )
        _setup_gke_loop_device_swap(pod)
        return

    disk = f"/dev/{disk_name}"
    logging.info(
        "[swap_encryption] GKE: swap target disk: %s  dmcrypt=%s",
        disk,
        _ENABLE_DMCRYPT.value,
    )

    # Clean up any stale mapping from a previous failed run.
    _pod_exec(
        pod,
        textwrap.dedent(f"""
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    dmsetup remove --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    wipefs -a {disk} 2>/dev/null || true
  """),
        ignore_failure=True,
    )

    if _ENABLE_DMCRYPT.value:
        # We cannot use cryptsetup open from inside a container because
        # libdevmapper calls dm_udev_wait() after creating the target, which
        # blocks on /run/udev/control.  That socket belongs to udevd which is
        # not running inside the container — so cryptsetup hangs forever.
        #
        # Instead we drive dmsetup directly with --noudevrules --noudevsync,
        # which skips all udev synchronisation, and call dmsetup mknodes to
        # ensure /dev/mapper/swap_encrypted appears without udev.
        #
        # insmod (not modprobe) loads the kernel module: modprobe also talks to
        # systemd-udevd and can deadlock from a container for the same reason.
        _pod_exec(
            pod,
            textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {disk})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{disk}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted
      swapon /dev/mapper/swap_encrypted
    """),
        )
        logging.info(
            "[swap_encryption] GKE: dm-crypt swap active on "
            "/dev/mapper/swap_encrypted"
        )
    else:
        # Encryption-disabled column of the test matrix
        _pod_exec(
            pod,
            textwrap.dedent(f"""
      mkswap {disk} && \\
      swapon {disk}
    """),
        )
        logging.info(
            "[swap_encryption] GKE: plain (unencrypted) swap active on %s", disk
        )


def _setup_gke_loop_device_swap(pod: str) -> None:
    """Plain loop-device swap for single-disk GKE nodes (no dedicated swap disk).

    Used when _setup_gke_hyperdisk_swap finds no dedicated second disk (e.g.
    n2-highmem-32 / n4-highmem-32 single-boot-disk nodes, regardless of image
    type).

    dm-crypt is skipped on this path for two reasons:
    1. On COS (Container-Optimised OS): the device-mapper kernel subsystem is
       inaccessible from inside a Kubernetes pod (even privileged).  Calls to
       cryptsetup/dmsetup block indefinitely and are killed by the PKB timeout.
       This is a deliberate COS security restriction, not a permissions issue.
    2. On UBUNTU_CONTAINERD: the loop device is created in the container
       namespace; its behaviour under nsenter (needed for dm-crypt on dedicated
       disks) is untested, so plain loop swap is used for safety.
    For dedicated block devices (hyperdisk, LSSD) nsenter into the host mount
    namespace works around the COS restriction (see _setup_gke_hyperdisk_swap).
    The loop device path skips dm-crypt on all image types; plain loop swap is
    used instead.

    Therefore this path uses a plain loop device as swap without dm-crypt.
    Phase 1 (fio) is skipped for plain loop devices — the goal is enc-on vs
    enc-off comparison, and fio on a plain loop device measures the backing
    filesystem rather than the swap stack.  Tiers 2–6 (stress-ng, Redis,
    kernel build, OpenSearch) run normally.

    For dm-crypt measurement on GCP use a machine type with local NVMe (LSSD)
    or provision a dedicated hyperdisk on a second disk slot (n4-highmem-32+).

    Improvements over the old /var path:
    - Backing file on /mnt/stateful_partition (ext4), not the container
      overlayfs — avoids overlayfs O_DIRECT limitation.
    - losetup --direct-io=on passes I/O through to the host ext4, reducing
      double-buffering for Tiers 2–6 workloads.
    """
    size_gb = _SWAP_SIZE_GB.value
    # /mnt/stateful_partition is ext4 on COS (mounted from the stateful
    # partition of the node's persistent disk).  It is NOT the container
    # overlay filesystem and is mounted into the pod via the DaemonSet
    # hostPath volume.
    backing = "/mnt/stateful_partition/pkb_swap_backing"

    # ── Step 0: detach any stale loop device from a previous failed run ───────
    _pod_exec(
        pod,
        textwrap.dedent(f"""
    losetup -j {backing} 2>/dev/null | awk -F: '{{print $1}}' | \
      while read dev
      do
        swapoff "$dev" 2>/dev/null || true
        losetup -d "$dev" 2>/dev/null || true
      done
    rm -f {backing}
  """),
        ignore_failure=True,
    )

    # ── Step 1: allocate backing file on stateful partition (ext4) ───────────
    logging.info(
        "[swap_encryption] GKE: creating %dG backing file on"
        " stateful_partition",
        size_gb,
    )
    # fallocate preallocates real ext4 blocks (avoids fragmentation during swap
    # I/O); truncate is the sparse fallback for filesystems where fallocate
    # fails.
    _pod_exec(
        pod,
        textwrap.dedent(f"""
    fallocate -l {size_gb}G {backing} 2>/dev/null || \\
      truncate -s {size_gb}G {backing}
  """),
    )

    # ── Step 2: loop device with direct-io passthrough ───────────────────────
    # --direct-io=on lets the loop driver pass O_DIRECT to the host ext4,
    # reducing double-buffering for workload I/O (kernel 5.x+, present on
    # GKE COS ≥ 1.29).
    loop_out, _ = _pod_exec(
        pod,
        textwrap.dedent(f"""
    LOOP=$(losetup -f) && \\
    losetup --direct-io=on "$LOOP" {backing} && \\
    echo "$LOOP"
  """),
    )
    loop_dev = loop_out.strip()
    if not loop_dev.startswith("/dev/loop"):
        raise RuntimeError(
            f"[swap_encryption] losetup failed – output: {loop_out!r}"
        )
    logging.info(
        "[swap_encryption] GKE: loop device: %s  direct-io=on", loop_dev
    )

    # ── Step 3: plain mkswap + swapon (dm-crypt skipped on loop devices) ────────
    _pod_exec(pod, f"mkswap {loop_dev}")
    _pod_exec(pod, f"swapon {loop_dev}")
    logging.warning(
        "[swap_encryption] GKE: plain loop swap active on %s "
        "(dm-crypt unavailable from COS pod — device-mapper is blocked by "
        "COS kernel namespace restrictions). "
        "Phase 1 (fio) will be skipped. "
        "Use a machine with LSSD (c4-*-lssd) or attach a dedicated second "
        "hyperdisk for dm-crypt measurement.",
        loop_dev,
    )


def _setup_gke_bootdisk_swap(pod: str) -> None:
    """Swap on the OS BOOT disk — methodology Table 0 rows 1-4.

    Creates a loop-backed swap file on /mnt/stateful_partition (the node's boot
    disk, whose type — pd-balanced or hyperdisk-balanced — is chosen at
    nodepool-creation time via --swap_encryption_boot_disk_type).  dm-crypt is
    layered on the loop device when --swap_encryption_enable_dmcrypt is set
    (encryption-on rows 2/4); otherwise plain swap is used (encryption-off rows
    1/3).

    Reuses the same loop-creation and dmsetup patterns as the LSSD/hyperdisk
    paths — no shared provider module is touched.  Requires an Ubuntu node image
    (dm-crypt from a pod is blocked on COS).
    """
    size_gb = _SWAP_SIZE_GB.value
    backing = "/mnt/stateful_partition/pkb_swap_backing"
    logging.info(
        "[swap_encryption] GKE: boot-disk swap (%dG backing, dmcrypt=%s)",
        size_gb,
        _ENABLE_DMCRYPT.value,
    )

    # Clean up any stale loop/mapping from a previous run.
    _pod_exec(
        pod,
        textwrap.dedent(f"""
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    dmsetup remove --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    losetup -j {backing} 2>/dev/null | awk -F: '{{print $1}}' | while read d
    do
      swapoff "$d" 2>/dev/null || true
      losetup -d "$d" 2>/dev/null || true
    done
    rm -f {backing}
  """),
        ignore_failure=True,
    )

    # Allocate the backing file on the boot-disk ext4 stateful partition.
    _pod_exec(
        pod,
        textwrap.dedent(f"""
    fallocate -l {size_gb}G {backing} 2>/dev/null || truncate -s {size_gb}G {backing}
  """),
    )

    loop_out, _ = _pod_exec(
        pod,
        textwrap.dedent(f"""
    LOOP=$(losetup -f) && losetup --direct-io=on "$LOOP" {backing} && echo "$LOOP"
  """),
    )
    loop_dev = (
        loop_out.strip().splitlines()[-1].strip() if loop_out.strip() else ""
    )
    if not loop_dev.startswith("/dev/loop"):
        raise RuntimeError(
            f"[swap_encryption] boot-disk losetup failed: {loop_out!r}"
        )
    logging.info("[swap_encryption] GKE: boot-disk loop device: %s", loop_dev)

    if _ENABLE_DMCRYPT.value:
        _pod_exec(
            pod,
            textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {loop_dev})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{loop_dev}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted
      swapon /dev/mapper/swap_encrypted
    """),
        )
        logging.info(
            "[swap_encryption] GKE: boot-disk dm-crypt swap active on "
            "/dev/mapper/swap_encrypted"
        )
    else:
        _pod_exec(
            pod,
            textwrap.dedent(f"""
      mkswap {loop_dev} && swapon {loop_dev}
    """),
        )
        logging.info(
            "[swap_encryption] GKE: boot-disk plain swap active on %s", loop_dev
        )


def _setup_gke_lssd_swap(pod: str) -> None:
    """Configure dm-crypt on LSSD RAID-0 array (go/gke-swap-lssd)."""
    logging.info("[swap_encryption] GKE: setting up LSSD RAID-0 swap")

    # Reused-node hygiene: a previous run on this node may have left an ACTIVE
    # dm-crypt swap (e.g. /dev/nvme0n1 └─swap_encrypted [SWAP]).  That makes the
    # LSSD look "unclean/busy" to the device selector below, which then wrongly
    # falls back to the hyperdisk path and tries the boot disk.  Tear down any
    # prior PKB swap mapping FIRST so the underlying LSSD is freed and selectable.
    _pod_exec(
        pod,
        textwrap.dedent("""
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    swapoff -a 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
  """),
        ignore_failure=True,
    )

    # Log the full block-device topology up front for diagnosis (every prior
    # swap failure traced back to picking the wrong device).
    topo, _ = _pod_exec(
        pod,
        "lsblk -o NAME,TYPE,SIZE,ROTA,MOUNTPOINT 2>/dev/null",
        ignore_failure=True,
    )
    logging.info(
        "[swap_encryption] block device topology:\n%s", (topo or "").strip()
    )

    # Identify candidate swap devices = whole disks that are NOT the boot/OS
    # disk.  We must NOT rely on a device name (boot disk enumerates as nvme0n1
    # on some nodes, nvme1n1 on others) and we cannot use `findmnt /` because the
    # container root is an overlay.  Instead we EXCLUDE any disk that:
    #   * has partition children (boot disk has p1/p14/p15/p16), or
    #   * has any mounted filesystem (itself or a child).
    # A raw local SSD intended for swap has neither.  This robustly prevents the
    # catastrophic bug where the 100 GB boot disk (root mounted) was RAIDed into
    # the swap device, yielding a non-functional swap (fio empty + stress OOM).
    lssd_out, _ = _pod_exec(
        pod,
        textwrap.dedent("""
        for d in $(lsblk -dno NAME,ROTA | awk '$2==0{print $1}')
        do
          if lsblk -no TYPE "/dev/$d" 2>/dev/null | grep -q '^part$'; then
            continue   # has partitions -> boot/OS disk
          fi
          if lsblk -no MOUNTPOINT "/dev/$d" 2>/dev/null | grep -q '[^[:space:]]'; then
            continue   # mounted somewhere -> not a free swap device
          fi
          echo "/dev/$d"
        done
      """),
        ignore_failure=True,
    )
    devices = [d.strip() for d in lssd_out.strip().splitlines() if d.strip()]
    if not devices:
        logging.warning(
            "[swap_encryption] No clean (unpartitioned, unmounted) local SSD"
            " found — falling back to hyperdisk swap path"
        )
        _setup_gke_hyperdisk_swap(pod)
        return

    device_list = " ".join(devices)
    n = len(devices)
    logging.info(
        "[swap_encryption] GKE: LSSD RAID-0 across %d clean device(s): "
        "%s  dmcrypt=%s",
        n,
        device_list,
        _ENABLE_DMCRYPT.value,
    )

    # Clean up stale mappings, RAID arrays, and GKE-managed mounts.
    #
    # GKE UBUNTU nodes run google-ssd-startup.service at boot which formats
    # local NVMe SSDs as ext4 and mounts them at /mnt/disks/ssd0 etc. even
    # when --local-nvme-ssd-block is set.  The mount makes the block device
    # busy so mdadm/wipefs fail silently (we had || true).  We must unmount
    # those paths first.  /proc-host/mounts reflects the host mount table
    # (hostPID:true + privileged gives us access).
    #
    # pkb_swap is the dm-crypt device created by the node startup script (for
    # single-LSSD nodes it holds /dev/nvme1n1 directly without an md0 layer).
    _pod_exec(
        pod,
        textwrap.dedent(f"""
    echo "[pkb-lssd-cleanup] /proc/mdstat:" >&2
    cat /proc/mdstat 2>/dev/null || true
    echo "[pkb-lssd-cleanup] dmsetup ls:" >&2
    dmsetup ls 2>/dev/null || true
    echo "[pkb-lssd-cleanup] /proc/swaps:" >&2
    cat /proc/swaps 2>/dev/null || true
    echo "[pkb-lssd-cleanup] host mounts on {device_list}:" >&2
    grep -E '{('|'.join(devices))}' /proc-host/mounts 2>/dev/null || true
    echo "[pkb-lssd-cleanup] sysfs holders:" >&2
    for dev in {device_list}
    do
      devname=$(basename "$dev")
      ls -1 /sys/block/$devname/holders/ 2>/dev/null | while read h
      do
        echo "[pkb-lssd-cleanup]   $dev held by $h" >&2
      done
    done
    echo "[pkb-lssd-cleanup] --- begin teardown ---" >&2
    for dev in {device_list}
    do
      test -b "$dev" || continue
      devname=$(basename "$dev")
      for holder in /sys/block/$devname/holders/*
      do
        test -e "$holder" || continue
        h=$(basename "$holder")
        echo "[pkb-lssd-cleanup] removing holder /dev/$h from $dev" >&2
        if echo "$h" | grep -q "^md"
        then
          mdadm --stop /dev/$h 2>/dev/null || true
        else
          dmsetup remove --force --noudevrules --noudevsync /dev/$h 2>/dev/null || true
        fi
      done
      mounts=$(awk -v d="$dev" '$1==d{{print $2}}' /proc-host/mounts 2>/dev/null || true)
      for mp in $mounts
      do
        echo "[pkb-lssd-cleanup] unmounting $mp from $dev" >&2
        umount -f "$mp" 2>/dev/null || true
      done
    done
    swapoff -a 2>/dev/null || true
    swapoff /dev/mapper/pkb_swap 2>/dev/null || true
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync pkb_swap 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    mdadm --stop --scan 2>/dev/null || true
    mdadm --zero-superblock {device_list} 2>/dev/null || true
    wipefs -a {device_list} 2>/dev/null || true
    echo "[pkb-lssd-cleanup] lsblk after wipefs:" >&2
    lsblk {device_list} 2>/dev/null || true
    partx -u {device_list} 2>/dev/null || true
    losetup -D 2>/dev/null || true
    rm -f /mnt/stateful_partition/pkb_swap.img 2>/dev/null || true
    sleep 2
  """),
        ignore_failure=True,
    )

    # Step 3: verify the devices are truly raw (unpartitioned).  On GKE Ubuntu
    # nodes the local NVMe device may be partitioned by node startup scripts
    # even when --local-nvme-ssd-block is specified.  The kernel refuses a
    # whole-disk exclusive open (DM_TABLE_LOAD → EBUSY) when any partition of
    # the disk is open by another process (e.g. the container overlay FS is
    # backed by nvme1n1p1).  Detect this and fall back to a loop device backed
    # by a file on /mnt/stateful_partition (which IS the SSD partition).
    raw_check_out, _ = _pod_exec(
        pod,
        textwrap.dedent(f"""
        for dev in {device_list}
        do
          if lsblk -ln -o TYPE "$dev" 2>/dev/null | grep -q '^part$'
          then
            echo "[pkb-lssd] $dev is partitioned — cannot use as raw block device" >&2
          else
            echo "$dev"
          fi
        done
      """),
        ignore_failure=True,
    )
    raw_devices = [
        d.strip() for d in raw_check_out.strip().splitlines() if d.strip()
    ]

    if not raw_devices:
        logging.info(
            "[swap_encryption] GKE: all LSSD devices are partitioned — "
            "falling back to loop device on /mnt/stateful_partition"
        )
        _setup_gke_lssd_stateful_loop_swap(pod)
        return

    # Use only raw (unpartitioned) devices going forward.
    devices = raw_devices
    device_list = " ".join(devices)
    n = len(devices)
    logging.info(
        "[swap_encryption] GKE: using %d raw LSSD device(s): %s  dmcrypt=%s",
        n,
        device_list,
        _ENABLE_DMCRYPT.value,
    )

    # For N=1 LSSD, skip mdadm entirely and target the raw device directly.
    # For N>1 we stripe across multiple NVMe devices.
    if n > 1:
        _pod_exec(
            pod,
            textwrap.dedent(f"""
      mdadm --create /dev/md0 --force \\
        --level=0 --raid-devices={n} \\
        {device_list}
      test -b /dev/md0 || {{ echo "mdadm: /dev/md0 not created" >&2; exit 1; }}
    """),
        )
        swap_block_dev = "/dev/md0"
    else:
        swap_block_dev = devices[0]
        logging.info(
            "[swap_encryption] GKE: single LSSD — skipping mdadm, "
            "using %s directly",
            swap_block_dev,
        )

    if _ENABLE_DMCRYPT.value:
        # Same dmsetup --noudevrules --noudevsync approach as _setup_gke_hyperdisk_swap.
        _pod_exec(
            pod,
            textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      udevadm control --stop-exec-queue 2>/dev/null || true
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {swap_block_dev})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{swap_block_dev}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      udevadm control --start-exec-queue 2>/dev/null || true
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted
      swapon /dev/mapper/swap_encrypted
    """),
        )
        logging.info(
            "[swap_encryption] GKE: LSSD dm-crypt swap active on %s",
            swap_block_dev,
        )
    else:
        _pod_exec(
            pod,
            textwrap.dedent(f"""
      mkswap {swap_block_dev}
      swapon {swap_block_dev}
    """),
        )
        logging.info(
            "[swap_encryption] GKE: LSSD plain swap active on %s",
            swap_block_dev,
        )


def _setup_gke_lssd_stateful_loop_swap(pod: str) -> None:
    """Set up swap on the LSSD partition via a loop device.

    Used when the local NVMe device is partitioned by GKE startup scripts
    and cannot be opened as a whole raw block device (DM_TABLE_LOAD EBUSY).
    The DaemonSet mounts /mnt/stateful_partition (hostPath) from the host's
    nvme1n1p1 — which is still local SSD storage.  We create a large file
    there and layer loop → dm-crypt → swap on top of it.
    """
    img_path = "/mnt/stateful_partition/pkb_swap.img"

    # Clean up any previous run artifacts.
    _pod_exec(
        pod,
        textwrap.dedent(f"""
    swapoff -a 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    losetup -D 2>/dev/null || true
    rm -f {img_path} 2>/dev/null || true
  """),
        ignore_failure=True,
    )

    # Determine file size: 80% of available space, at least 16 GB.
    size_out, _ = _pod_exec(
        pod,
        f"df -P /mnt/stateful_partition | awk 'NR==2{{print $4}}'",
        ignore_failure=True,
    )
    avail_kb = int(size_out.strip() or "0")
    swap_gb = max(16, int(avail_kb * 0.8 / 1024 / 1024))
    logging.info(
        "[swap_encryption] GKE: LSSD stateful-loop: %d GB image at %s",
        swap_gb,
        img_path,
    )

    # Allocate file (fallocate is instant on ext4; dd fallback for others).
    _pod_exec(
        pod,
        textwrap.dedent(f"""
    fallocate -l {swap_gb}G {img_path} 2>/dev/null || \\
      dd if=/dev/zero of={img_path} bs=1G count={swap_gb}
    chmod 600 {img_path}
    losetup --direct-io=on -f {img_path}
  """),
        timeout=300,
    )

    loop_out, _ = _pod_exec(
        pod,
        f"losetup -j {img_path} | awk -F: '{{print $1}}' | head -1",
        ignore_failure=True,
    )
    loop_dev = loop_out.strip()
    if not loop_dev.startswith("/dev/loop"):
        raise RuntimeError(
            f"[swap_encryption] losetup failed for {img_path} — got:"
            f" {loop_out!r}"
        )
    logging.info(
        "[swap_encryption] GKE: LSSD stateful-loop device: %s", loop_dev
    )

    if _ENABLE_DMCRYPT.value:
        _pod_exec(
            pod,
            textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      udevadm control --stop-exec-queue 2>/dev/null || true
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {loop_dev})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{loop_dev}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      udevadm control --start-exec-queue 2>/dev/null || true
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted
      swapon /dev/mapper/swap_encrypted
    """),
        )
        logging.info(
            "[swap_encryption] GKE: LSSD stateful-loop dm-crypt swap active "
            "on %s → %s",
            img_path,
            loop_dev,
        )
    else:
        _pod_exec(
            pod,
            textwrap.dedent(f"""
      mkswap {loop_dev}
      swapon {loop_dev}
    """),
        )
        logging.info(
            "[swap_encryption] GKE: LSSD stateful-loop plain swap active "
            "on %s → %s",
            img_path,
            loop_dev,
        )


_IO2_VOLUME_ID = ""  # set by _ensure_io2_volume; serial-based detection


def _ensure_io2_volume() -> None:
    """Create + attach a dedicated io2 EBS volume to the benchmark node so the
    io2 test-matrix row swaps on real io2 hardware-encrypted storage.

    No-op unless --swap_encryption_swap_type=io2 on an AWS/EKS cluster.
    Best-effort: logs and returns on failure.  Stashes the created volume id in
    _IO2_VOLUME_ID for serial-based device detection in _setup_eks_io2_swap.
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
            logging.info(
                "[swap_encryption] io2 volume UNENCRYPTED (baseline row)"
            )
        create_args += ["--query", "VolumeId", "--output", "text"]
        vol_id, _, vrc = vm_util.IssueCommand(
            base + create_args, raise_on_failure=False
        )
        vol_id = (vol_id or "").strip()
        if vrc != 0 or not vol_id.startswith("vol-"):
            logging.warning(
                "[swap_encryption] io2 create-volume failed: %r", vol_id
            )
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
        logging.warning(
            "[swap_encryption] io2 attach error (continuing): %s", e
        )


def _setup_eks_swap(pod: str) -> None:
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
        _setup_eks_instance_store_swap(pod)
    elif swap_type == "io2":
        _setup_eks_io2_swap(pod)
    else:
        logging.warning(
            "[swap_encryption] Unknown EKS swap type %s – fallback", swap_type
        )
        _setup_eks_instance_store_swap(pod)


def _setup_eks_instance_store_swap(pod: str) -> None:
    """Swap on AWS NVMe Instance Store (Nitro hardware-offloaded encryption)."""
    logging.info("[swap_encryption] EKS: setting up Instance Store swap")

    # Find the Instance Store NVMe device (not the root EBS volume)
    nvme_out, _ = _pod_exec(
        pod,
        "nvme list 2>/dev/null | awk '/Instance Storage/{print $1}' | head -1"
        " || lsblk -d -o NAME,MODEL | grep -i 'instance\\|nvme' | grep -v"
        " 'nvme0' | awk '{print \"/dev/\"$1}' | head -1",
        ignore_failure=True,
    )
    device = nvme_out.strip()
    if not device:
        # Common Instance Store device paths on AWS
        for candidate in ["/dev/nvme1n1", "/dev/nvme2n1", "/dev/xvdb"]:
            exists_out, _ = _pod_exec(
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
        _setup_plain_swap_file(pod, _SWAP_SIZE_GB.value)
        return

    logging.info("[swap_encryption] EKS: Instance Store device: %s", device)

    # Nitro encrypts all Instance Store writes automatically.
    # No additional cryptsetup required.
    _pod_exec(
        pod,
        textwrap.dedent(f"""
    mkswap {device} && \\
    swapon {device}
  """),
    )
    logging.info(
        "[swap_encryption] EKS: Instance Store swap active on %s", device
    )


def _setup_eks_io2_swap(pod: str) -> None:
    """Swap on AWS EBS io2 volume – apples-to-apples comparison vs GKE hyperdisk.

    EBS io2 volumes on Nitro instances are encrypted at rest by AWS KMS (if
    enabled on the volume) or via Nitro-level hardware encryption.  No additional
    cryptsetup is needed here; we simply format the attached data disk as swap.

    Device discovery order:
      1. Match the io2 volume created by _ensure_io2_volume() by its NVMe serial
         (serial == volume id without the dash).  This is unambiguous and never
         picks the root disk or the instance store regardless of nvmeXn1
         enumeration order on Nitro.
      2. First non-root EBS ("Elastic Block Store") block device that is not
         currently mounted.
    """
    logging.info("[swap_encryption] EKS: setting up io2 EBS swap")

    # Identify root device so we can exclude it.
    root_out, _ = _pod_exec(
        pod,
        "lsblk -no pkname $(findmnt -n -o SOURCE /) 2>/dev/null || echo"
        " nvme0n1",
        ignore_failure=True,
    )
    root_base = root_out.strip() or "nvme0n1"

    # Identify the io2 volume UNAMBIGUOUSLY by its NVMe serial == volume id.
    # An EBS NVMe device's serial equals the volume id minus the dash
    # (vol-0abc... -> serial vol0abc...).
    device = ""
    target = _IO2_VOLUME_ID.replace("-", "")
    if target:
        ser_out, _ = _pod_exec(
            pod,
            "for d in /sys/block/nvme*n1; do "
            '[ -e "$d" ] || continue; '
            's=$(cat "$d/device/serial" 2>/dev/null | tr -d "-" | tr -d " "); '
            f'[ "$s" = "{target}" ] && {{ echo "/dev/$(basename "$d")"; break;'
            " }; "
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
        # Fallback: first non-root EBS device, excluding any device that is
        # currently mounted (root) or already active swap.
        disk_out, _ = _pod_exec(
            pod,
            "for d in /sys/block/nvme*n1 /sys/block/xvd[b-z]"
            " /sys/block/sd[b-z];"
            ' do [ -e "$d" ] || continue; n=$(basename "$d"); [ "$n" ='
            f' "{root_base}" ] && continue; m=$(cat "$d/device/model"'
            " 2>/dev/null);"
            ' echo "$m" | grep -qi "Elastic Block Store" || continue;'
            " mnt=$(lsblk"
            ' -no MOUNTPOINT "/dev/$n" 2>/dev/null | tr -d " "); [ -n "$mnt"'
            " ] &&"
            ' continue; echo "/dev/$n"; break; done',
            ignore_failure=True,
        )
        device = disk_out.strip()
        if device:
            logging.info(
                "[swap_encryption] EKS: io2 fallback EBS device: %s", device
            )

    if not device:
        logging.warning(
            "[swap_encryption] No io2 EBS disk found – creating plain swapfile"
        )
        _setup_plain_swap_file(pod, _SWAP_SIZE_GB.value)
        return

    logging.info("[swap_encryption] EKS: io2 EBS device: %s", device)

    # EBS io2 encryption is handled at the AWS level (Nitro / KMS).
    out, _ = _pod_exec(
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


def _setup_plain_swap_file(pod: str, size_gb: int) -> None:
    """Fallback: create a loop-device-backed swapfile.

    A plain file on overlayfs (the container root) cannot be used as swap —
    the kernel rejects it with EINVAL.  Routing it through a loop device
    presents a proper block device to the mm subsystem and succeeds.
    """
    logging.info("[swap_encryption] Creating %dGB loop-device swap", size_gb)
    _pod_exec(
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


def _enable_zswap(pod: str) -> None:
    """Enable zswap with lz4 compressor and 20% pool limit inside the pod."""
    logging.info("[swap_encryption] Enabling zswap (lz4, 20%% pool)")
    for cmd in [
        "echo 1      > /sys/module/zswap/parameters/enabled",
        "echo lz4    > /sys/module/zswap/parameters/compressor",
        "echo 20     > /sys/module/zswap/parameters/max_pool_percent",
        "echo z3fold > /sys/module/zswap/parameters/zpool",
    ]:
        _pod_exec(pod, cmd, ignore_failure=True)


def _phase1_fio(
    pod: str, swap_dev: str, base_meta: dict
) -> list[sample.Sample]:
    """Run fio directly on the swap block device for raw I/O characterisation.

    Skipped only for an UNINTENTIONAL loop fallback (a single-disk node with no
    dedicated swap disk, where fio on the loop would measure the boot ext4
    filesystem rather than the swap stack).  When the user explicitly selects the
    boot_disk target (--swap_encryption_swap_type=boot_disk, methodology rows
    1-4), the loop over the boot disk IS the device under test, so fio runs and
    characterises it.

    For dedicated second disks (hyperdisk, LSSD, NVMe) direct I/O is always
    used and swap is restored (mkswap + swapon) after the fio run.
    To get fio results use c4-*-lssd (local NVMe) or
    --swap_encryption_add_swap_disk to provision a dedicated second disk.
    """
    if swap_dev.startswith("/dev/loop") and _SWAP_TYPE.value != "boot_disk":
        logging.warning(
            "[swap_encryption] Phase 1 (fio) SKIPPED for plain loop device %s"
            " (unintentional single-disk fallback). fio on a loop-backed device"
            " measures the underlying ext4 filesystem (stateful_partition), not"
            " the swap stack. Use c4-*-lssd, --swap_encryption_add_swap_disk,"
            " or --swap_encryption_swap_type=boot_disk for fio data.",
            swap_dev,
        )
        return []

    results = []

    _pod_exec(pod, f"swapoff {swap_dev}", ignore_failure=True)

    # Pre-fill device so read tests have real data (avoids zero-block optimisation
    # by the storage controller skewing read latency measurements).
    # Cap at 20 GiB — enough to warm up the dm-crypt pipeline and cover the fio
    # runtime window.  Writing 100% of a 500 GiB hyperdisk takes ~500+ seconds
    # at provisioned throughput, which exceeds the PKB command timeout.
    # Timeout: 20 GiB / ~150 MB/s (conservative dm-crypt write rate) + 60 s buffer.
    _PREFILL_GIB = 20
    prefill_timeout = (
        _PREFILL_GIB * 1024 // 150 + 60
    )  # ~197 s, rounds up to ~200 s
    prefill_timeout = max(prefill_timeout, 300)  # floor at 5 min
    logging.info(
        "[swap_encryption] Pre-filling %d GiB of %s", _PREFILL_GIB, swap_dev
    )
    # No --output-format=json for prefill; we only care that it completes.
    # Still use --output to avoid streaming large stdout over the websocket.
    _pod_exec(
        pod,
        (
            f"fio --name=prefill --filename={swap_dev} --ioengine=libaio"
            f" --direct=1 --rw=write --bs=1m --size={_PREFILL_GIB}g --verify=0"
            " --output=/tmp/pkb_fio_prefill.log"
        ),
        timeout=prefill_timeout,
        ignore_failure=True,
    )

    # Each fio job: runtime + 90 s buffer (run + JSON write + file read).
    # We write fio output to a file inside the pod and retrieve it in a second
    # short-lived kubectl exec, because:
    #   - A single 120 s kubectl exec session over GKE websocket can be reset
    #     by the control-plane load balancer mid-stream ("connection reset by
    #     peer"), losing the output.
    #   - Separating the long run from the short file-read gives each exec a
    #     much shorter window, avoiding the keepalive timeout.
    fio_run_timeout = _FIO_RUNTIME_SEC.value + 90
    fio_read_timeout = 60  # just a cat of the JSON file

    for name, rw, bs, depth, label in _FIO_JOBS:
        logging.info("[swap_encryption] fio: %s", name)
        out_file = f"/tmp/pkb_fio_{name}.json"
        # Remove any stale output first so a parse can never silently reuse a
        # previous job's/run's result (rules out byte-identical results between
        # runs being a caching artifact rather than a true device ceiling).
        _pod_exec(
            pod,
            f"rm -f {out_file}",
            ignore_failure=True,
            _retries=0,
            timeout=15,
        )
        run_cmd = (
            f"fio --name={name} --filename={swap_dev} "
            "--ioengine=libaio --direct=1 --verify=0 --randrepeat=0 "
            f"--bs={bs} --iodepth={depth} --rw={rw} "
            f"--time_based --runtime={_FIO_RUNTIME_SEC.value}s "
            f"--output-format=json --output={out_file}"
        )
        _, err = _pod_exec(
            pod,
            run_cmd,
            timeout=fio_run_timeout,
            ignore_failure=True,
            _retries=0,
        )
        if "connection reset by peer" in err:
            logging.warning(
                "[swap_encryption] fio %s: kubectl exec connection "
                "reset; result may be incomplete",
                name,
            )
        out, _ = _pod_exec(
            pod,
            f'cat {out_file} 2>/dev/null || echo ""',
            timeout=fio_read_timeout,
            ignore_failure=True,
        )
        results += _parse_fio_json(out, name, label, base_meta)

    # fio prefill overwrites the entire device, destroying the mkswap header.
    # Re-stamp and re-enable before the remaining phases need active swap.
    _pod_exec(
        pod,
        f"mkswap {swap_dev} && swapon {swap_dev}",
        ignore_failure=True,
        timeout=120,
    )
    return results


def _parse_fio_json(
    stdout: str, job_name: str, label: str, base_meta: dict
) -> list[sample.Sample]:
    """Parse fio JSON output into PKB Samples."""
    results = []
    try:
        data = json.loads(stdout)
    except (json.JSONDecodeError, ValueError):
        logging.warning(
            "[swap_encryption] fio JSON parse failed for %s", job_name
        )
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
                sample.Sample(
                    f"{job_name}_{direction}_lat_mean", lat_mean, "us", m
                ),
                sample.Sample(
                    f"{job_name}_{direction}_lat_p50", lat_p50, "us", m
                ),
                sample.Sample(
                    f"{job_name}_{direction}_lat_p99", lat_p99, "us", m
                ),
                sample.Sample(
                    f"{job_name}_{direction}_lat_p999", lat_p999, "us", m
                ),
            ]
    return results


def _parse_vm_bytes_to_mb(vm_bytes: str) -> float:
    """Parse a vm-bytes string like '28G', '512M', '1024k' into megabytes."""
    vm_bytes = vm_bytes.strip()
    if not vm_bytes:
        return 0.0
    suffix = vm_bytes[-1].upper()
    try:
        value = float(vm_bytes[:-1])
    except ValueError:
        return 0.0
    if suffix == "G":
        return value * 1024.0
    elif suffix == "M":
        return value
    elif suffix == "K":
        return value / 1024.0
    elif suffix == "T":
        return value * 1024.0 * 1024.0
    else:
        # Assume bytes
        try:
            return float(vm_bytes) / (1024.0 * 1024.0)
        except ValueError:
            return 0.0


def _per_worker_vm_bytes(total_vm_bytes: str, workers: int) -> str:
    """Split a *total* vm-bytes target across N stress-ng --vm workers.

    stress-ng allocates ``--vm-bytes`` PER worker, so ``--vm N --vm-bytes B``
    touches ``N * B`` of memory.  Every vm_bytes value in this benchmark (the
    --swap_encryption_stress_vm_bytes flag and the _autoscale_vm_bytes result)
    represents the intended *combined* footprint, as documented on
    --swap_encryption_stress_vm_workers ("workers divide vm_bytes equally ...
    the combined in-flight footprint equals vm_bytes").  We therefore divide by
    the worker count before handing the value to stress-ng; otherwise N>1
    workers allocate N x the target and the kernel OOM-kills the whole pod
    (observed as stress-ng rc=137, after which all later phases fail with
    "pods not found").

    Returns a stress-ng-friendly ``<int>M`` string (megabytes), floored to at
    least 1M.
    """
    workers = max(1, int(workers))
    total_mb = _parse_vm_bytes_to_mb(total_vm_bytes)
    if total_mb <= 0:
        # Unparseable — fall back to letting stress-ng divide nothing rather than
        # silently changing behaviour; the caller's value is passed through.
        return total_vm_bytes
    per_worker_mb = max(1, int(total_mb / workers))
    return f"{per_worker_mb}M"


def _cgroup_swap_limit_mb(pod: str) -> float:
    """Return the swap budget (in MB) that the benchmark cgroup can actually use.

    GKE sets the per-container cgroup v2 ``memory.swap.max`` to 0, so even though
    the node advertises a large swap device the container cannot page anything
    out.  Sizing stress-ng against the *node* swap total in that case guarantees
    an OOM kill.  This probe finds the swap budget of *our* cgroup so the caller
    can size against reality.

    We locate our own cgroup from the host-mounted /sys by finding the
    ``cgroup.procs`` file that lists this shell's PID — ``hostPID: true`` means
    ``$$`` is a host-namespace PID that appears in those files, and the
    kubectl-exec'd shell shares the container's cgroup with stress-ng.

    Returns:
      ``float('inf')`` when swap is uncapped (``max``); the limit in MB when
      capped to a finite value; ``0.0`` when swap is fully locked
      (``memory.swap.max == 0``); ``-1.0`` when the limit could not be read (the
      caller then falls back to the legacy node-total behaviour).
    """
    probe = textwrap.dedent("""
    mypid=$$
    for procf in $(find /sys/fs/cgroup -path '*kubepods*' -name cgroup.procs 2>/dev/null)
    do
      if grep -qx "$mypid" "$procf" 2>/dev/null
      then
        d=$(dirname "$procf")
        if [ -f "$d/memory.swap.max" ]
        then
          echo "V2=$(cat "$d/memory.swap.max" 2>/dev/null)"
        elif [ -f "$d/memory.memsw.limit_in_bytes" ] && [ -f "$d/memory.limit_in_bytes" ]
        then
          echo "MEMSW=$(cat "$d/memory.memsw.limit_in_bytes" 2>/dev/null) MEM=$(cat "$d/memory.limit_in_bytes" 2>/dev/null)"
        fi
        break
      fi
    done
  """)
    try:
        out, _ = _pod_exec(pod, probe, timeout=20, ignore_failure=True)
    except Exception as e:  # pylint: disable=broad-except
        logging.warning(
            "[swap_encryption] cgroup swap-limit probe failed: %s", e
        )
        return -1.0

    text = (out or "").strip()
    m = re.search(r"V2=(\S+)", text)
    if m:
        val = m.group(1)
        if val == "max":
            return float("inf")
        try:
            return int(val) / (1024.0 * 1024.0)
        except ValueError:
            return -1.0
    # cgroup v1: the combined RAM+swap ceiling is memsw; swap budget = memsw-mem.
    m = re.search(r"MEMSW=(\S+)\s+MEM=(\S+)", text)
    if m:
        try:
            memsw = int(m.group(1))
            mem = int(m.group(2))
        except ValueError:
            return -1.0
        # A near-2^63 sentinel means "unlimited" in cgroup v1.
        if memsw >= (1 << 62):
            return float("inf")
        return max(0.0, (memsw - mem) / (1024.0 * 1024.0))
    return -1.0


def _autoscale_vm_bytes(pod: str, vm_bytes: str) -> str:
    """Ensure vm_bytes forces real swap I/O without hard-crashing the container.

    Strategy
    --------
    We want stress-ng to overflow into swap so that dm-crypt / Nitro encryption
    overhead is actually measured.  Two competing constraints apply:

    1. vm_bytes must exceed available RAM so that anonymous pages are paged out
       to the swap device.  A value below ~95 % of RAM fits entirely in memory
       and produces swap_out_pages_per_sec = 0 (benchmark defeats itself).

    2. vm_bytes must not be so large that the kernel OOM-kills the whole
       container before any meaningful swap activity is recorded.

    Target formula
    --------------
    target = RAM + min(swap_size × 0.25, 64 GB)

    This guarantees at least 25 % of the swap device is actively exercised
    (measured swap I/O) while keeping the allocation safely within what the
    kernel can page out given the available swap space.  The 64 GB cap prevents
    extremely large targets on machines with huge swap devices.

    On large-RAM machines (e.g. n4-highmem-32, 252 GB) the old 110%-of-RAM
    formula only overflowed by ~25 GB; with sequential write64 patterns the
    kernel handled that via LRU page eviction without actually hitting the swap
    device, yielding swap_out = 0.  The new formula forces a much larger working
    set into swap.

    Hard ceiling
    ------------
    Regardless of the formula, cap at RAM + swap_size - 4 GB (4 GB headroom)
    to avoid exhausting the swap device and triggering kernel panics.
    """
    try:
        meminfo_out, _ = _pod_exec(pod, "cat /proc/meminfo", timeout=15)
        node_ram_kb = 0
        swap_total_kb = 0
        for line in meminfo_out.splitlines():
            if line.startswith("MemTotal:"):
                parts = line.split()
                if len(parts) >= 2:
                    node_ram_kb = int(parts[1])
            elif line.startswith("SwapTotal:"):
                parts = line.split()
                if len(parts) >= 2:
                    swap_total_kb = int(parts[1])
            if node_ram_kb and swap_total_kb:
                break

        if node_ram_kb <= 0:
            logging.warning(
                "[swap_encryption] Could not read MemTotal; using vm_bytes=%s",
                vm_bytes,
            )
            return vm_bytes

        node_ram_mb = node_ram_kb / 1024.0
        swap_total_mb = swap_total_kb / 1024.0
        requested_mb = _parse_vm_bytes_to_mb(vm_bytes)
        if requested_mb <= 0:
            return vm_bytes

        # The node may advertise a large SwapTotal while THIS cgroup is forbidden
        # from using it (GKE sets memory.swap.max=0 per container).  Size against
        # the swap the cgroup can actually reach, not the node total — otherwise a
        # value like 32G OOM-kills the pod the instant it exceeds RAM.
        cgroup_swap_mb = _cgroup_swap_limit_mb(pod)
        usable_swap_mb = (
            swap_total_mb  # default / legacy when probe is inconclusive
        )
        if cgroup_swap_mb == 0.0:
            # Swap is fully locked.  Cap the working set just under RAM so the pod
            # survives, and mark the run degraded: swap-encryption overhead cannot be
            # measured when the cgroup cannot page out.
            safe_gb = max(1, int(node_ram_mb * 0.9 / 1024))
            msg = (
                "cgroup swap is locked (memory.swap.max=0); the"
                f" {swap_total_mb/1024:.0f} GB node swap device is unreachable."
                f" Capping stress-ng vm_bytes {vm_bytes} → {safe_gb}G (0.9 x"
                " RAM) to keep the pod alive — swap-encryption overhead will"
                " NOT be measured this run"
            )
            logging.error("[swap_encryption] %s", msg)
            _degraded_reasons.append(msg)
            return f"{safe_gb}G"
        if 0.0 < cgroup_swap_mb < float("inf"):
            # cgroup permits a finite swap budget smaller than the device.
            usable_swap_mb = min(swap_total_mb, cgroup_swap_mb)
        # cgroup_swap_mb == inf -> swap fully usable (node total stands)
        # cgroup_swap_mb == -1  -> undetermined; fall back to node total (legacy)

        # Desired overflow: 25% of usable swap capped at 64 GB, minimum 4 GB.
        overflow_mb = max(min(usable_swap_mb * 0.25, 64.0 * 1024), 4.0 * 1024)
        target_mb = node_ram_mb + overflow_mb

        # Hard ceiling: never exceed RAM + usable swap − 4 GB headroom.
        if usable_swap_mb > 0:
            ceiling_mb = node_ram_mb + usable_swap_mb - 4096.0
            target_mb = min(target_mb, ceiling_mb)
        else:
            # No usable swap at all (and not the locked-at-0 case handled above):
            # keep the working set just under RAM.
            target_mb = min(target_mb, node_ram_mb * 0.9)

        target_gb = max(
            1, int(target_mb / 1024)
        )  # floor to GB for a clean flag

        if requested_mb < node_ram_mb * 0.95:
            new_vm_bytes = f"{target_gb}G"
            logging.warning(
                "[swap_encryption] Auto-scaling vm_bytes UP: %s → %s (RAM %.0f"
                " GB, swap %.0f GB; original value would not trigger swap)",
                vm_bytes,
                new_vm_bytes,
                node_ram_mb / 1024,
                swap_total_mb / 1024,
            )
            return new_vm_bytes

        if requested_mb > target_mb:
            new_vm_bytes = f"{target_gb}G"
            logging.warning(
                "[swap_encryption] Capping vm_bytes DOWN: %s → %s (RAM %.0f GB,"
                " swap %.0f GB; original value risks swap exhaustion)",
                vm_bytes,
                new_vm_bytes,
                node_ram_mb / 1024,
                swap_total_mb / 1024,
            )
            return new_vm_bytes

        return vm_bytes
    except Exception as e:  # pylint: disable=broad-except
        logging.warning(
            "[swap_encryption] _autoscale_vm_bytes failed (%s); using %s",
            e,
            vm_bytes,
        )
        return vm_bytes


def _get_stress_vm_method(pod: str) -> str:
    """Detect the best --vm-method argument for stress-ng on this node.

    stress-ng vm-method support varies by version and distro:
    - Older Ubuntu / some GKE images: supports 'mmap'
    - Newer Ubuntu on n4-highmem-32 (kernel 6.8+ GKE): 'mmap' removed; supports
      'write64', 'rand-set', etc.

    We prefer 'mmap' (lowest overhead, no kernel structure cycling), fall back to
    'write64' (simple sequential writes, universally available), then 'rand-set',
    and if none are listed we return '' so callers omit the --vm-method flag
    entirely (stress-ng then uses its compiled-in default).

    NOTE on forcing swap (two independent requirements):
    (a) The working set must exceed RAM.  Without --vm-keep each worker re-mmaps
        and re-touches its full slice every iteration, so all
        --swap_encryption_stress_vm_workers slices are simultaneously resident and
        the combined footprint exceeds RAM (run 910c8da5 swapped ~10k pages/s with
        write64 and no --vm-keep).  Adding --vm-keep made stress-ng reuse one
        quiescent mapping, the resident set plateaued below RAM, and the gate
        fired — so we must NOT pass --vm-keep.
    (b) The workers must stay BUSY for the whole phase.  Do NOT pass --vm-hang 0:
        stress-ng documents "--vm-hang 0" as "sleep for an INFINITE time before
        unmapping", so each worker wrote its slice once and then slept for the
        rest of the run — usr+sys CPU was ~10 s out of 300 s and si/so stayed 0
        (runs 14907cff, config1/111, even with KSM disabled and rand-set).
        Omitting --vm-hang entirely lets the workers loop continuously, keeping
        the slices hot so the over-RAM remainder pages to swap throughout.

    Result is cached in _stress_vm_method so the detection kubectl exec only runs
    once per benchmark run.
    """
    if _stress_vm_method:
        return _stress_vm_method[0]

    try:
        # stress-ng prints its valid vm-methods to stdout when given an invalid one.
        out, _, _ = kubectl.RunKubectlCommand(
            [
                "exec",
                (_active_pod[0] if _active_pod else pod),
                "-n",
                _DS_NAMESPACE,
                "--",
                "bash",
                "-c",
                (
                    "stress-ng --vm 1 --vm-bytes 1M --vm-method __invalid__"
                    " --timeout 1s 2>&1 || true"
                ),
            ],
            raise_on_failure=False,
            timeout=15,
        )
        combined = out.lower()
        # Prefer rand-set: random access keeps every page of each worker's slice
        # hot (no cold pages behind a sequential write pointer to reclaim) and
        # writes non-identical data (so KSM cannot merge the workers' regions).
        # write64 is sequential and was empirically reclaimed / merged, leaving the
        # resident set below RAM and swap_out ~0.
        if "rand-set" in combined:
            method = "rand-set"
        elif "mmap" in combined:
            method = "mmap"
        elif "write64" in combined:
            method = "write64"
        else:
            method = ""  # omit flag; use stress-ng default
        logging.info(
            "[swap_encryption] stress-ng vm-method detected: %r",
            method or "(default)",
        )
    except Exception as e:  # pylint: disable=broad-except
        logging.warning(
            "[swap_encryption] vm-method detection failed (%s); using rand-set",
            e,
        )
        method = "rand-set"

    _stress_vm_method.append(method)
    return method


def _stress_vm_method_flag(pod: str) -> str:
    """Return the --vm-method <method> flag string, or empty string if none."""
    method = _get_stress_vm_method(pod)
    return f"--vm-method {method}" if method else ""


def _phase2a_cpu_overhead(pod: str, base_meta: dict) -> list[sample.Sample]:
    """Measure CPU cost of dm-crypt / Nitro while stress-ng drives swap I/O.

    If --swap_encryption_stress_vm_bytes_list is set the phase is run once per
    listed intensity value so that a full pressure-curve is captured (gap 5).
    Otherwise the single value from --swap_encryption_stress_vm_bytes is used.

    Auto-scaling: if the requested vm_bytes is less than 95% of node RAM, it is
    automatically increased to 110% of node RAM so that swap is actually
    triggered on large-RAM machines (e.g. n4-highmem-32 with 256 GB).
    """
    # Build the list of vm-bytes intensities to sweep (gap 5)
    if _STRESS_VM_BYTES_LIST.value.strip():
        intensities = [
            v.strip()
            for v in _STRESS_VM_BYTES_LIST.value.split(",")
            if v.strip()
        ]
    else:
        intensities = [_STRESS_VM_BYTES.value]

    results = []
    for vm_bytes in intensities:
        scaled = _autoscale_vm_bytes(pod, vm_bytes)
        logging.info(
            "[swap_encryption] Phase 2a: stress-ng intensity %s", scaled
        )
        results += _run_cpu_overhead_sweep(pod, base_meta, scaled)
    return results


def _run_cpu_overhead_sweep(
    pod: str, base_meta: dict, vm_bytes: str
) -> list[sample.Sample]:
    """Phase 2a stressor sweep, WITH RETRY for flaky swap.

    Driving the multi-worker rand-set working set past RAM into swap is
    empirically non-deterministic on these nodes: the SAME config produced
    ~670k pages/s on some runs and <300 on others.  So we retry: if an attempt
    completes but peak swap-out is below the threshold (and it did not OOM),
    reclaim memory and re-run, keeping the BEST attempt.  An OOM, or a peak
    at/above threshold, ends the retries immediately.
    """
    meta = dict(base_meta, phase="cpu_overhead", stress_vm_bytes=vm_bytes)
    timeout = _STRESS_TIMEOUT_SEC.value
    interval = 2
    n_samples = timeout // interval + 10
    vmstat_log = f"/tmp/pkb_vmstat_{vm_bytes}.log"
    pidstat_log = f"/tmp/pkb_pidstat_{vm_bytes}.log"
    workers = max(1, _STRESS_VM_WORKERS.value)
    per_worker = _per_worker_vm_bytes(vm_bytes, workers)
    min_so = _MIN_SWAP_OUT_PAGES.value
    method_flag = _stress_vm_method_flag(pod)
    max_attempts = 3
    best = None

    for attempt in range(1, max_attempts + 1):
        t0 = time.time()
        stress_out, _ = _pod_exec(
            pod,
            textwrap.dedent(f"""
      echo 2 > /sys/kernel/mm/ksm/run 2>/dev/null || true
      echo 0 > /sys/kernel/mm/ksm/run 2>/dev/null || true
      sysctl -w vm.swappiness=100 >/dev/null 2>&1 || true
      PKB_MCG=$(awk -F: '/^0::/{{print $3}}' /proc/self/cgroup 2>/dev/null)
      echo "[pkb] phase2a attempt={attempt}/{max_attempts} ksm_run=$(cat /sys/kernel/mm/ksm/run 2>/dev/null || echo n/a) swappiness=$(cat /proc/sys/vm/swappiness 2>/dev/null) MemAvailable_kB=$(awk '/MemAvailable/{{print $2}}' /proc/meminfo) memory.swap.max=$(cat /sys/fs/cgroup$PKB_MCG/memory.swap.max 2>/dev/null || echo n/a) workers={workers} per_worker={per_worker}"
      vmstat {interval} {n_samples} > {vmstat_log} 2>&1 &
      VMSTAT_PID=$!
      pidstat -u {interval} {n_samples} -p ALL > {pidstat_log} 2>&1 &
      PISTAT_PID=$!
      stress-ng --vm {workers} \\
        --vm-bytes {per_worker} \\
        {method_flag} \\
        --timeout {timeout}s \\
        --metrics-brief 2>&1 || true
      kill $VMSTAT_PID $PISTAT_PID 2>/dev/null || true
    """),
            timeout=timeout + 60,
            ignore_failure=True,
        )
        elapsed = time.time() - t0

        completed_cleanly = (
            "successful run completed" in stress_out.lower()
            or "metrics-brief" in stress_out.lower()
            or "bogo-ops" in stress_out.lower()
        )
        oom_killed = (not completed_cleanly) and elapsed < timeout * 0.8
        vmstat_out, _ = _pod_exec(pod, f"cat {vmstat_log}", ignore_failure=True)
        pidstat_out, _ = _pod_exec(
            pod, f"cat {pidstat_log}", ignore_failure=True
        )
        vmstat_samples = _parse_vmstat(vmstat_out, meta)
        swap_out_max = max(
            (
                s.value
                for s in vmstat_samples
                if s.metric
                in ("swap_out_pages_per_sec", "swap_out_pages_per_sec_max")
            ),
            default=0.0,
        )
        bogo = None
        for line in stress_out.splitlines():
            mm = re.search(r"vm\s+\d+\s+(\d+)\s+\S+\s+bogo-ops", line)
            if mm:
                bogo = float(mm.group(1))
                break
        logging.info(
            "[swap_encryption] Phase 2a attempt %d/%d: peak swap-out "
            "%.0f pages/s (completed=%s, oom=%s)",
            attempt,
            max_attempts,
            swap_out_max,
            completed_cleanly,
            oom_killed,
        )
        if best is None or swap_out_max > best["swap_out_max"]:
            best = dict(
                elapsed=elapsed,
                oom_killed=oom_killed,
                swap_out_max=swap_out_max,
                vmstat_samples=vmstat_samples,
                pidstat_out=pidstat_out,
                bogo=bogo,
            )
        if oom_killed or swap_out_max >= min_so:
            break
        if attempt < max_attempts:
            logging.warning(
                "[swap_encryption] Phase 2a swap-out %.0f < %d threshold "
                "— reclaiming and retrying (%d/%d)",
                swap_out_max,
                min_so,
                attempt + 1,
                max_attempts,
            )
            _pod_exec(
                pod,
                textwrap.dedent("""
        echo -1000 > /proc/self/oom_score_adj 2>/dev/null || true
        pkill -9 stress-ng 2>/dev/null || true
        sleep 3; sync; echo 1 > /proc/sys/vm/drop_caches 2>/dev/null || true
      """),
                ignore_failure=True,
                timeout=60,
            )

    # Emit samples from the BEST attempt.
    results = [
        sample.Sample("stress_ng_duration_sec", best["elapsed"], "s", meta),
        sample.Sample(
            "stress_ng_completed",
            0.0 if best["oom_killed"] else 1.0,
            "status",
            meta,
        ),
    ]
    if best["bogo"] is not None:
        results.append(
            sample.Sample("stress_ng_bogo_ops", best["bogo"], "ops", meta)
        )
    results += best["vmstat_samples"]
    results += _parse_pidstat(best["pidstat_out"], meta)

    # Swap-activity gate: a completed run that moved ~no pages to swap never
    # exercised the encrypted swap path (the headline numbers would be hollow).
    if best["oom_killed"]:
        msg = (
            f"stress-ng (vm_bytes={vm_bytes}) was OOM-killed — the cgroup could"
            " not page anonymous memory out to swap; swap-encryption overhead"
            " was not measured"
        )
        logging.error("[swap_encryption] %s", msg)
        _degraded_reasons.append(msg)
    elif best["swap_out_max"] < min_so:
        msg = (
            f"stress-ng (vm_bytes={vm_bytes}) peak swap-out was only "
            f'{best["swap_out_max"]:.0f} pages/s (< {min_so} threshold) after '
            f"{max_attempts} attempts — the working set never meaningfully "
            f"paged to swap. Check vm_bytes vs RAM and the swap device"
        )
        logging.error("[swap_encryption] %s", msg)
        _degraded_reasons.append(msg)

    return results


def _parse_vmstat(output: str, base_meta: dict) -> list[sample.Sample]:
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

    def _mean(lst):
        return sum(lst) / len(lst) if lst else 0.0

    def _peak(lst):
        return max(lst) if lst else 0.0

    total_active = [u + s + w for u, s, w in zip(us_vals, sy_vals, wa_vals)]

    return [
        # Swap rates
        sample.Sample("swap_in_pages_per_sec", _mean(si_vals), "pages/s", meta),
        sample.Sample(
            "swap_in_pages_per_sec_max", _peak(si_vals), "pages/s", meta
        ),
        sample.Sample(
            "swap_out_pages_per_sec", _mean(so_vals), "pages/s", meta
        ),
        sample.Sample(
            "swap_out_pages_per_sec_max", _peak(so_vals), "pages/s", meta
        ),
        # Total CPU utilisation (gap 1)
        sample.Sample("total_cpu_pct_avg", _mean(total_active), "%", meta),
        sample.Sample("total_cpu_pct_max", _peak(total_active), "%", meta),
        # System (kernel) time % – encryption overhead signal (gap 2)
        sample.Sample("system_time_pct_avg", _mean(sy_vals), "%", meta),
        sample.Sample("system_time_pct_max", _peak(sy_vals), "%", meta),
        # User and I/O-wait for completeness
        sample.Sample("user_cpu_pct_avg", _mean(us_vals), "%", meta),
        sample.Sample("iowait_cpu_pct_avg", _mean(wa_vals), "%", meta),
    ]


def _parse_pidstat(output: str, base_meta: dict) -> list[sample.Sample]:
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


def _launch_confined_bg_stress(pod: str, timeout_s: int, logfile: str) -> None:
    """Launch the Phase 2b/3a background swap stressor confined to its OWN
    memory-capped cgroup, so it drives swap pressure WITHOUT starving the
    concurrent foreground workload (fio / Redis) or OOM-killing the pod.

    On a small node (config1, 30 GB) a flat 32 GB stressor plus a concurrent
    workload exhausts RAM faster than the kernel pages out, and the OOM killer
    takes the foreground process (the under-pressure app_io fio died with
    rc=137).  Confining the stressor to memory.max = 60% of RAM (with unlimited
    swap) makes it page within its own budget; the other ~40% of RAM stays free
    for the workload, and if the stressor overruns its cap only IT is killed —
    never the pod or the workload.

    Config-2 safety: on a 256 GB node, 60% = ~150 GB, far above the 32 GB
    stressor, so the cap is never reached and behaviour is unchanged.
    Best-effort: if the cgroup can't be created the stressor still runs in the
    main cgroup (degrades to prior behaviour, not worse).  MemTotal is read with
    grep/cut (no awk) to keep this clear of f-string brace escaping.
    """
    method = _stress_vm_method_flag(pod)
    vm_bytes = _STRESS_VM_BYTES.value
    _pod_exec(
        pod,
        textwrap.dedent(f"""
    nohup bash -c '
      BG=/sys/fs/cgroup/pkb_bgstress
      mkdir -p "$BG" 2>/dev/null || true
      echo +memory > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null || true
      echo max > "$BG/memory.swap.max" 2>/dev/null || true
      MT_KB=$(grep -m1 MemTotal /proc/meminfo | tr -s " " | cut -d" " -f2)
      echo $(( MT_KB * 1024 * 60 / 100 )) > "$BG/memory.max" 2>/dev/null || true
      echo $$ > "$BG/cgroup.procs" 2>/dev/null || true
      exec stress-ng --vm 1 --vm-bytes {vm_bytes} {method} --timeout {timeout_s}s
    ' >{logfile} 2>&1 &
    disown
    echo STRESS_STARTED
  """),
        timeout=30,
    )


def _set_memory_high_guard(pod: str, fraction: float = 0.9) -> None:
    """Cap the container cgroup ``memory.high`` at `fraction` x RAM.

    Phases 2b (I/O interference) and 3a (Redis) run a background stressor *and* a
    concurrent foreground workload (an 8 GB fio file / a Redis dataset).  On a
    small-RAM node (config1, 30 GB) their combined footprint exceeds RAM and the
    hard OOM killer (``memory.max``) terminates the pod (rc=137), wiping out both
    phases.  ``memory.high`` is a soft limit: when the cgroup crosses it the
    kernel reclaims and *swaps* aggressively (throttling the cgroup) instead of
    killing it — which is exactly the swap pressure these phases want to create.

    Config-2 safety: this is a no-op in effect on large-RAM nodes.  On
    n4-highmem-32 (256 GB) the 32 GB background workload never approaches 0.9 x
    256 GB = 230 GB, so the soft limit is never crossed and behaviour is
    unchanged.  Phase 2a is deliberately NOT guarded (it works on both configs).
    Best-effort; any failure is ignored.
    """
    _pod_exec(
        pod,
        textwrap.dedent(f"""
    PKB_MCG=$(awk -F: '/^0::/{{print $3}}' /proc/self/cgroup 2>/dev/null)
    MT_KB=$(awk '/MemTotal/{{print $2}}' /proc/meminfo)
    HIGH=$(( MT_KB * 1024 / 100 * {int(fraction * 100)} ))
    if [ -n "$PKB_MCG" ] && [ -f "/sys/fs/cgroup$PKB_MCG/memory.high" ]; then
      echo $HIGH > "/sys/fs/cgroup$PKB_MCG/memory.high" 2>/dev/null \
        && echo "[pkb] memory.high set to $HIGH bytes ({int(fraction * 100)}% RAM) — pod will swap, not OOM" \
        || echo "[pkb] WARNING: could not set memory.high" >&2
    fi
  """),
        ignore_failure=True,
        timeout=30,
        _retries=0,
    )


def _reset_memory_high_guard(pod: str) -> None:
    """Restore ``memory.high`` to ``max`` after a guarded phase."""
    _pod_exec(
        pod,
        textwrap.dedent("""
    PKB_MCG=$(awk -F: '/^0::/{print $3}' /proc/self/cgroup 2>/dev/null)
    if [ -n "$PKB_MCG" ] && [ -f "/sys/fs/cgroup$PKB_MCG/memory.high" ]; then
      echo max > "/sys/fs/cgroup$PKB_MCG/memory.high" 2>/dev/null || true
    fi
  """),
        ignore_failure=True,
        timeout=30,
        _retries=0,
    )


def _phase2b_io_interference(pod: str, base_meta: dict) -> list[sample.Sample]:
    """Quantify drop in application I/O when swap is under simultaneous pressure."""
    results = []
    # IMPORTANT: keep this OFF tmpfs.  /tmp is RAM-backed (tmpfs/overlay), so an
    # 8 GB fio file there consumes 8 GB of RAM and OOM-kills the pod on a small
    # node (config1, rc=137 at "Laying out IO file") before any swap pressure is
    # even applied.  /mnt/stateful_partition is the node's persistent boot disk
    # (hostPath mount) — the file lives on disk, not RAM, and the fio results
    # then measure real disk I/O under swap pressure, which is the intent.
    app_file = "/mnt/stateful_partition/pkb_app_io"
    timeout = _STRESS_TIMEOUT_SEC.value
    meta = dict(base_meta, phase="io_interference")

    # Relieve memory pressure via swap rather than the OOM killer (see helper).
    # No-op on large-RAM nodes; prevents the config1 Phase 2b OOM (rc=137).
    _set_memory_high_guard(pod)

    # Ensure fio is available — apt-get may have failed during DaemonSet init.
    _pod_exec(
        pod,
        textwrap.dedent("""
    command -v fio >/dev/null 2>&1 || {
      apt-get install -y -qq fio 2>/dev/null || true
    }
  """),
        ignore_failure=True,
        timeout=120,
    )

    # Reclaim node memory BEFORE creating the test file.  By this point Phase 2a
    # has hard-swapped the node and Phase 3c's OpenSearch (which runs first) may
    # have left a multi-GB JVM footprint; on a 30 GB node the file create then
    # gets OOM-killed (rc=137) at the NODE level — which neither --direct=1 nor
    # the cgroup memory.high guard can prevent (those are cgroup/page-cache
    # tools, not node-eviction controls).  Kill any leftover stressors/servers,
    # flush dirty pages, and drop caches so the node starts Phase 2b clean.
    _pod_exec(
        pod,
        textwrap.dedent("""
    pkill -9 stress-ng 2>/dev/null || true
    pkill -9 -f 'opensearch|elasticsearch' 2>/dev/null || true
    pkill -9 redis-server 2>/dev/null || true
    sync
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
    sleep 2
    echo "[pkb] pre-2b MemAvailable_kB=$(awk '/MemAvailable/{print $2}' /proc/meminfo) SwapFree_kB=$(awk '/SwapFree/{print $2}' /proc/meminfo)"
  """),
        ignore_failure=True,
        timeout=60,
    )

    # Create the test file on the persistent disk (see app_file note above).
    # --direct=1 (O_DIRECT, ext4 supports it) bypasses the page cache.  Size is
    # kept at 4 GB (not 8) so the create + the concurrent background stressor
    # cannot exhaust a 30 GB node even with swap already in use.
    _pod_exec(
        pod,
        (
            f"fio --name=create --filename={app_file} "
            "--rw=write --bs=1m --size=4G --verify=0 --direct=1"
        ),
        timeout=600,
        ignore_failure=True,
    )

    def _run_app_fio(pressure_label: str) -> list[sample.Sample]:
        # --direct=1 (O_DIRECT) avoids page-cache buildup; ext4 on the persistent
        # disk supports it.  --size=4G matches the file created above.  This
        # measures the disk's I/O under swap pressure directly.
        cmd = (
            f"fio --name=app_io --filename={app_file} "
            "--ioengine=libaio --direct=1 "
            "--rw=randrw --bs=4k --iodepth=32 --size=4G --verify=0 "
            "--time_based --runtime=60s --output-format=json"
        )
        # ignore_failure=True: fio rc=137 is expected when the pod is OOM-evicted
        # under heavy swap pressure.  _pod_exec handles recovery; callers rely on
        # _parse_fio_json returning [] on empty/bad output rather than an exception.
        out, _ = _pod_exec(pod, cmd, ignore_failure=True)
        return _parse_fio_json(
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
    # Confined background stressor: pages within a 60%-RAM cgroup so it can't
    # OOM the concurrent app_io fio on a small node (see helper).
    _launch_confined_bg_stress(pod, timeout, "/tmp/pkb_stress_io.log")
    time.sleep(10)  # let swap pressure build
    results += _run_app_fio("with_swap_pressure")

    # Stop background stress-ng.  If the pod was OOM-evicted while fio ran,
    # stress-ng is already dead — kill is a no-op and we skip the long wait.
    # _retries=0: no recovery here; the first Phase 3a command will recover
    # the pod properly if needed (and it already waits for /tmp/pkb_ready).
    _pod_exec(
        pod,
        "pkill -9 stress-ng 2>/dev/null || true",
        ignore_failure=True,
        _retries=0,
        timeout=15,
    )
    _reset_memory_high_guard(pod)
    return results


_INSTANCE_PRICE_USD_PER_HR: dict[str, float] = {
    # GCP  (on-demand, us-central1 unless noted)
    "c4-standard-8-lssd": 0.5888,  # 8 vCPU, 32 GB RAM + 1×375 GB LSSD
    "c4-standard-8": 0.5008,  # 8 vCPU, 32 GB RAM, no LSSD
    "n4-highmem-32": 3.0256,  # 32 vCPU, 256 GB RAM
    "n2-highmem-32": 2.5216,  # 32 vCPU, 256 GB RAM
    "n2-standard-32": 1.5264,  # 32 vCPU, 120 GB RAM
    "z3-highmem-8": 2.7248,  # 8 vCPU + 4× LSSD
    # AWS
    "i4i.4xlarge": 1.4960,  # 16 vCPU, 128 GB RAM, NVMe Instance Store
    "i4i.2xlarge": 0.7480,
    "m6id.4xlarge": 0.9072,  # 16 vCPU, 64 GB RAM, NVMe Instance Store
    "m6i.4xlarge": 0.7680,  # 16 vCPU, 64 GB RAM, no Instance Store
    "r6i.4xlarge": 1.0080,  # 16 vCPU, 128 GB RAM, no Instance Store
}


def _collect_cost_sample(
    pod: str, elapsed_sec: float, base_meta: dict
) -> list[sample.Sample]:
    """Emit a cost_estimate_usd sample for the benchmark run (gap 7).

    Instance type is read from cloud metadata inside the pod.  Price is looked
    up from _INSTANCE_PRICE_USD_PER_HR; if unknown, the sample is omitted and
    a warning is logged.

    Args:
      pod: Benchmark pod name.
      elapsed_sec: Wall-clock seconds the benchmark phases took.
      base_meta: Shared metadata dict.

    Returns:
      A list of zero or one sample.Sample.
    """
    # Detect instance type from cloud metadata
    instance_type = ""

    # GCP: machine type is the last segment of the metadata URL value
    gcp_type_out, _ = _pod_exec(
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
        aws_type_out, _ = _pod_exec(
            pod,
            "curl -s -m 3 --fail "
            "http://169.254.169.254/latest/meta-data/instance-type "
            '2>/dev/null || echo ""',
            ignore_failure=True,
        )
        instance_type = aws_type_out.strip()

    # Allow explicit override (useful when running on custom/renamed machine
    # types or when the pod was unavailable during cost collection).
    if _INSTANCE_SIZE_LABEL.value:
        instance_type = _INSTANCE_SIZE_LABEL.value

    # Last resort: fall back to the benchmark machine type flag.  This ensures
    # cost tracking works even when the pod was evicted before cost collection
    # ran (in which case the metadata curl above returned empty).
    if not instance_type and _BENCHMARK_MACHINE_TYPE.value:
        instance_type = _BENCHMARK_MACHINE_TYPE.value
        logging.info(
            "[swap_encryption] Instance type from metadata unavailable; using"
            " --swap_encryption_benchmark_machine_type=%s for cost tracking",
            instance_type,
        )

    price = _INSTANCE_PRICE_USD_PER_HR.get(instance_type)
    if price is None:
        logging.warning(
            '[swap_encryption] Unknown instance type "%s" – skipping cost'
            " sample. Add it to _INSTANCE_PRICE_USD_PER_HR to enable cost"
            " tracking.",
            instance_type,
        )
        return []

    hours = elapsed_sec / 3600.0
    cost = hours * price
    meta = dict(
        base_meta,
        instance_type=instance_type,
        price_usd_per_hr=price,
        benchmark_elapsed_sec=round(elapsed_sec, 1),
    )
    return [sample.Sample("cost_estimate_usd", cost, "USD", meta)]


def _detect_swap_device(pod: str) -> str:
    """Return the active swap device path on the cluster node."""
    if _SWAP_DEVICE.value:
        return _SWAP_DEVICE.value

    # /proc/swaps is the source of truth: it lists the swap device that is
    # ACTUALLY active.  We must NOT just `test -e /dev/mapper/swap_encrypted`,
    # because a stale dm-crypt mapping from a previous run on a reused node can
    # still exist as a /dev node while being non-functional (fio/swapoff then
    # fail with "No such device or address").  So read the active device from
    # /proc/swaps first; only fall back to the mapper path if /proc/swaps is
    # somehow empty but the mapper is genuinely present.
    dm_out, _ = _pod_exec(
        pod,
        textwrap.dedent("""
        ACTIVE=$(awk 'NR==2{print $1}' /proc/swaps 2>/dev/null)
        if [ -n "$ACTIVE" ]
        then
          echo "$ACTIVE"
        elif test -e /dev/mapper/swap_encrypted
        then
          echo /dev/mapper/swap_encrypted
        fi
      """),
        ignore_failure=True,
    )
    dev = dm_out.strip().splitlines()[-1].strip() if dm_out.strip() else ""
    if dev:
        return dev
    raise ValueError(
        "No active swap device found in the benchmark pod. "
        "Use --swap_encryption_device to specify one."
    )


def _build_metadata(pod: str, swap_dev: str) -> dict:
    """Collect node environment, encryption type, and config into a dict."""

    kernel_out, _ = _pod_exec(pod, "uname -r", ignore_failure=True)
    mem_out, _ = _pod_exec(
        pod,
        "awk '/MemTotal/{print $2}' /proc/meminfo",
        ignore_failure=True,
    )
    swap_out, _ = _pod_exec(
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

    # Encryption type — key off dm-crypt presence + the swap target, NOT the
    # device path.  A GKE plain Local SSD is /dev/nvme0n1 but is NOT Nitro-
    # encrypted; only the AWS targets (instance_store / io2) are.
    enc = "unknown"
    if "/dev/mapper/" in swap_dev:
        table_out, _ = _pod_exec(
            pod,
            f'dmsetup table {swap_dev.split("/")[-1]} 2>/dev/null || echo ""',
            ignore_failure=True,
        )
        enc = "dm-crypt-plain" if "crypt" in table_out.lower() else "dm-other"
    elif _SWAP_TYPE.value in ("instance_store", "io2"):
        enc = "nitro_hardware_offload"  # AWS: encrypted by the Nitro card
    elif not _ENABLE_DMCRYPT.value:
        enc = "none"  # GKE plain swap (encryption OFF)

    cloud = _detect_cloud(pod)

    # Gap 6: instance size label for multi-size comparison runs.
    # If the flag is set use it directly; otherwise try to read it from
    # cloud metadata so that the field is always populated.
    instance_label = _INSTANCE_SIZE_LABEL.value
    if not instance_label:
        gcp_type_out, _ = _pod_exec(
            pod,
            "curl -s -m 3 --fail"
            " http://metadata.google.internal/computeMetadata/v1/instance/machine-type"
            ' -H "Metadata-Flavor: Google" 2>/dev/null || echo ""',
            ignore_failure=True,
        )
        if gcp_type_out.strip():
            instance_label = gcp_type_out.strip().split("/")[-1]
    if not instance_label:
        aws_type_out, _ = _pod_exec(
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
        # Test-matrix columns: storage target, encryption on/off, image, IOPS
        "storage_target": _SWAP_TYPE.value,
        "boot_disk_type": _BOOT_DISK_TYPE.value,
        "dmcrypt_enabled": _ENABLE_DMCRYPT.value,
        "node_image_type": _NODE_IMAGE_TYPE.value,
        "boot_disk_iops_target": _BOOT_DISK_IOPS.value,
        "benchmark_machine_type": _BENCHMARK_MACHINE_TYPE.value,
        # Other config
        "zswap_enabled": _ENABLE_ZSWAP.value,
        "min_free_kbytes": _MIN_FREE_KBYTES.value,
        "fio_runtime_sec": _FIO_RUNTIME_SEC.value,
        # Requested config value only.  The *effective* stress-ng footprint may
        # be autoscaled per node (see _autoscale_vm_bytes); Phase 2a records the
        # actual value it ran with as 'stress_vm_bytes' so the two never conflict.
        "stress_vm_bytes_requested": _STRESS_VM_BYTES.value,
        "stress_vm_bytes_list": _STRESS_VM_BYTES_LIST.value,
        "stress_timeout_sec": _STRESS_TIMEOUT_SEC.value,
        "nodepool": _NODEPOOL.value,
    }
