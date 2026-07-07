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
"""Utility helpers for swap_encryption_benchmark.

Contains metadata collection and cost estimation helpers used by
swap_encryption_benchmark.  Extracted so they can be tested independently
without FLAG reads inside the functions.

Mirrors the pattern used by fio/utils.py for the fio benchmark.
"""

import logging
from typing import Any

from perfkitbenchmarker import sample


# ---------------------------------------------------------------------------
# On-demand instance pricing (USD/hr) for cost_estimate_usd sample.
# ---------------------------------------------------------------------------

_INSTANCE_PRICE_USD_PER_HR: dict[str, float] = {
    # GCP  (on-demand, us-central1 unless noted)
    'c4-standard-8-lssd': 0.5888,  # 8 vCPU, 32 GB RAM + 1×375 GB LSSD
    'c4-standard-8': 0.5008,        # 8 vCPU, 32 GB RAM, no LSSD
    'n4-highmem-32': 3.0256,        # 32 vCPU, 256 GB RAM
    'n2-highmem-32': 2.5216,        # 32 vCPU, 256 GB RAM
    'n2-standard-32': 1.5264,       # 32 vCPU, 120 GB RAM
    'z3-highmem-8': 2.7248,         # 8 vCPU + 4× LSSD
    # AWS
    'i4i.4xlarge': 1.4960,          # 16 vCPU, 128 GB RAM, NVMe Instance Store
    'i4i.2xlarge': 0.7480,
    'm6id.4xlarge': 0.9072,         # 16 vCPU, 64 GB RAM, NVMe Instance Store
    'm6i.4xlarge': 0.7680,          # 16 vCPU, 64 GB RAM, no Instance Store
    'r6i.4xlarge': 1.0080,          # 16 vCPU, 128 GB RAM, no Instance Store
}

_GCP_INSTANCE_TYPE_CMD = (
    'curl -s -m 3 --fail'
    ' http://metadata.google.internal/computeMetadata/v1/instance/machine-type'
    ' -H "Metadata-Flavor: Google" 2>/dev/null || echo ""'
)
_AWS_INSTANCE_TYPE_CMD = (
    'curl -s -m 3 --fail '
    'http://169.254.169.254/latest/meta-data/instance-type '
    '2>/dev/null || echo ""'
)


def BuildMetadata(
    daemonset,
    swap_dev: str,
    *,
    swap_type: str,
    enable_dmcrypt: bool,
    node_image_type: str = '',
    boot_disk_type: str = '',
    boot_disk_iops: int = 0,
    benchmark_machine_type: str = '',
    enable_zswap: bool = False,
    min_free_kbytes: int = 0,
    fio_runtime_sec: int = 60,
    stress_vm_bytes: str = '',
    stress_vm_bytes_list: list | None = None,
    stress_timeout_sec: int = 0,
    nodepool: str = '',
    instance_size_label: str = '',
    benchmark_name: str = 'swap_encryption',
) -> dict[str, Any]:
  """Collect node environment, encryption config, and run parameters.

  All FLAG-derived values are passed as explicit keyword arguments so that
  this function is testable without a FLAGS mock.

  Args:
    daemonset: Active SwapDaemonSet resource; used for pod exec queries.
    swap_dev: Active swap device path, e.g. /dev/mapper/swap_encrypted.
    swap_type: --swap_encryption_swap_type value.
    enable_dmcrypt: --swap_encryption_enable_dmcrypt value.
    node_image_type: --swap_encryption_node_image_type value.
    boot_disk_type: --swap_encryption_boot_disk_type value.
    boot_disk_iops: --swap_encryption_boot_disk_iops value.
    benchmark_machine_type: --swap_encryption_benchmark_machine_type value.
    enable_zswap: --swap_encryption_enable_zswap value.
    min_free_kbytes: --swap_encryption_min_free_kbytes value.
    fio_runtime_sec: --swap_encryption_fio_runtime_sec value.
    stress_vm_bytes: --swap_encryption_stress_vm_bytes value.
    stress_vm_bytes_list: --swap_encryption_stress_vm_bytes_list value.
    stress_timeout_sec: --swap_encryption_stress_timeout_sec value.
    nodepool: --swap_encryption_nodepool value.
    instance_size_label: --swap_encryption_instance_size_label value.
    benchmark_name: BENCHMARK_NAME constant from benchmark module.

  Returns:
    Dict of metadata keys suitable for use as Sample.metadata.
  """
  if stress_vm_bytes_list is None:
    stress_vm_bytes_list = []

  kernel_out, _ = daemonset.PodExec('uname -r', ignore_failure=True)
  mem_out, _ = daemonset.PodExec(
      "awk '/MemTotal/{print $2}' /proc/meminfo",
      ignore_failure=True,
  )
  swap_out, _ = daemonset.PodExec(
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
  enc = 'unknown'
  if '/dev/mapper/' in swap_dev:
    table_out, _ = daemonset.PodExec(
        f'dmsetup table {swap_dev.split("/")[-1]} 2>/dev/null || echo ""',
        ignore_failure=True,
    )
    enc = 'dm-crypt-plain' if 'crypt' in table_out.lower() else 'dm-other'
  elif swap_type in ('instance_store', 'io2'):
    enc = 'nitro_hardware_offload'  # AWS: encrypted by the Nitro card
  elif not enable_dmcrypt:
    enc = 'none'  # GKE plain swap (encryption OFF)

  cloud = daemonset.DetectCloud()

  instance_label = instance_size_label
  if not instance_label:
    gcp_type_out, _ = daemonset.PodExec(
        _GCP_INSTANCE_TYPE_CMD,
        ignore_failure=True,
    )
    if gcp_type_out.strip():
      instance_label = gcp_type_out.strip().split('/')[-1]
  if not instance_label:
    aws_type_out, _ = daemonset.PodExec(
        _AWS_INSTANCE_TYPE_CMD,
        ignore_failure=True,
    )
    instance_label = aws_type_out.strip()

  return {
      'benchmark': benchmark_name,
      'execution_mode': 'kubernetes_privileged_pod',
      'cloud': cloud,
      'instance_size': instance_label,
      'kernel_version': kernel_out.strip(),
      'host_memory_gb': mem_gb,
      'swap_device': swap_dev,
      'swap_size_gb': swap_gb,
      'swap_encryption': enc,
      # Test-matrix columns: storage target, encryption on/off, image, IOPS
      'storage_target': swap_type,
      'boot_disk_type': boot_disk_type,
      'dmcrypt_enabled': enable_dmcrypt,
      'node_image_type': node_image_type,
      'boot_disk_iops_target': boot_disk_iops,
      'benchmark_machine_type': benchmark_machine_type,
      # Other config
      'zswap_enabled': enable_zswap,
      'min_free_kbytes': min_free_kbytes,
      'fio_runtime_sec': fio_runtime_sec,
      # Requested config value only.
      'stress_vm_bytes_requested': stress_vm_bytes,
      'stress_vm_bytes_list': stress_vm_bytes_list,
      'stress_timeout_sec': stress_timeout_sec,
      'nodepool': nodepool,
  }


def CollectCostSample(
    daemonset,
    elapsed_sec: float,
    base_meta: dict[str, Any],
    instance_size_label: str = '',
    benchmark_machine_type: str = '',
) -> list[sample.Sample]:
  """Emit a cost_estimate_usd Sample for the benchmark run.

  Instance type is read from cloud metadata inside the pod.  Price is looked
  up from _INSTANCE_PRICE_USD_PER_HR; if unknown, the sample is omitted and
  a warning is logged.

  Args:
    daemonset: Active SwapDaemonSet resource.
    elapsed_sec: Wall-clock seconds the benchmark phases took.
    base_meta: Shared metadata dict.
    instance_size_label: --swap_encryption_instance_size_label value.
    benchmark_machine_type: --swap_encryption_benchmark_machine_type value.

  Returns:
    A list of zero or one sample.Sample.
  """
  instance_type = ''

  # GCP: machine type is the last segment of the metadata URL value
  gcp_type_out, _ = daemonset.PodExec(
      _GCP_INSTANCE_TYPE_CMD,
      ignore_failure=True,
  )
  if gcp_type_out.strip():
    instance_type = gcp_type_out.strip().split('/')[-1]

  if not instance_type:
    # AWS: instance-type is a plain string
    aws_type_out, _ = daemonset.PodExec(
        _AWS_INSTANCE_TYPE_CMD,
        ignore_failure=True,
    )
    instance_type = aws_type_out.strip()

  # Allow explicit override.
  if instance_size_label:
    instance_type = instance_size_label

  # Last resort: fall back to the benchmark machine type flag.
  if not instance_type and benchmark_machine_type:
    instance_type = benchmark_machine_type
    logging.info(
        '[swap_encryption] Metadata unavailable; using machine_type=%s',
        instance_type,
    )

  price = _INSTANCE_PRICE_USD_PER_HR.get(instance_type)
  if price is None:
    logging.info(
        '[swap_encryption] No price for type "%s"; skipping cost sample.',
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
  return [sample.Sample('cost_estimate_usd', cost, 'USD', meta)]
