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

from typing import Any

# ---------------------------------------------------------------------------
# On-demand instance pricing (USD/hr) for cost_estimate_usd sample.
# ---------------------------------------------------------------------------


def BuildMetadata(
    daemonset: Any,
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
    stress_vm_bytes_list: list[str] | None = None,
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

  resource_meta = daemonset.GetResourceMetadata()
  kernel_version = resource_meta.get('kernel_version', 'unknown')
  mem_gb = resource_meta.get('memory_gb', 0)
  swap_gb = resource_meta.get('swap_gb', 0)

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

  cloud = getattr(daemonset, 'CLOUD', 'unknown')
  if not instance_size_label:
    instance_size_label = benchmark_machine_type

  return {
      'benchmark': benchmark_name,
      'execution_mode': 'kubernetes_privileged_pod',
      'cloud': cloud,
      'instance_size': instance_size_label,
      'kernel_version': kernel_version,
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
