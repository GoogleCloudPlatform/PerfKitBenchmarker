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
"""Tests for swap_encryption_utils helpers.

Covers BuildMetadata() and CollectCostSample() without any FLAGS mocking,
since both functions accept explicit keyword arguments.
"""

import unittest
from unittest import mock

from perfkitbenchmarker.linux_benchmarks.kubernetes.swap_encryption import utils
from tests import pkb_common_test_case


def _make_daemonset(responses: dict[str, tuple[str, str]] | None = None):
  """Return a mock daemonset whose PodExec and DetectCloud are pre-configured.

  Args:
    responses: mapping from substring of cmd → (stdout, stderr) tuple.
      DetectCloud always returns 'gcp'.
  """
  ds = mock.MagicMock()
  ds.DetectCloud.return_value = 'gcp'

  def _pod_exec(cmd, **_):
    if responses:
      for key, val in responses.items():
        if key in cmd:
          return val
    return ('', '')

  ds.PodExec.side_effect = _pod_exec
  return ds


class BuildMetadataTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for swap_encryption_utils.BuildMetadata()."""

  def _basic_ds(self):
    return _make_daemonset({
        'uname': ('5.15.0-gke-123\n', ''),
        'MemTotal': ('33292816\n', ''),  # ~32 GiB
        'proc/swaps': ('32768\n', ''),  # ~32 GiB swap
    })

  def test_metadata_contains_required_keys(self):
    """Metadata dict contains all required benchmark keys."""
    ds = self._basic_ds()
    meta = utils.BuildMetadata(
        ds,
        '/dev/mapper/swap_encrypted',
        swap_type='hyperdisk',
        enable_dmcrypt=True,
    )
    for key in (
        'benchmark',
        'cloud',
        'swap_device',
        'swap_encryption',
        'storage_target',
        'dmcrypt_enabled',
        'kernel_version',
    ):
      self.assertIn(key, meta, f'Missing key: {key}')

  def test_swap_device_stored_verbatim(self):
    ds = self._basic_ds()
    meta = utils.BuildMetadata(
        ds,
        '/dev/nvme1n1',
        swap_type='instance_store',
        enable_dmcrypt=False,
    )
    self.assertEqual(meta['swap_device'], '/dev/nvme1n1')

  def test_dm_crypt_device_queries_dmsetup(self):
    ds = _make_daemonset({
        'uname': ('5.15.0\n', ''),
        'MemTotal': ('0\n', ''),
        'proc/swaps': ('0\n', ''),
        'dmsetup table': ('0 67108864 crypt aes-xts-plain64 0 8:16 0\n', ''),
    })
    meta = utils.BuildMetadata(
        ds,
        '/dev/mapper/swap_encrypted',
        swap_type='hyperdisk',
        enable_dmcrypt=True,
    )
    self.assertEqual(meta['swap_encryption'], 'dm-crypt-plain')

  def test_non_mapper_device_with_dmcrypt_off_returns_none(self):
    ds = self._basic_ds()
    meta = utils.BuildMetadata(
        ds,
        '/dev/nvme0n1',
        swap_type='lssd',
        enable_dmcrypt=False,
    )
    self.assertEqual(meta['swap_encryption'], 'none')

  def test_instance_store_type_returns_nitro(self):
    ds = self._basic_ds()
    meta = utils.BuildMetadata(
        ds,
        '/dev/nvme1n1',
        swap_type='instance_store',
        enable_dmcrypt=False,
    )
    self.assertEqual(meta['swap_encryption'], 'nitro_hardware_offload')

  def test_io2_type_returns_nitro(self):
    ds = self._basic_ds()
    meta = utils.BuildMetadata(
        ds,
        '/dev/xvdf',
        swap_type='io2',
        enable_dmcrypt=False,
    )
    self.assertEqual(meta['swap_encryption'], 'nitro_hardware_offload')

  def test_explicit_instance_size_label_used(self):
    ds = self._basic_ds()
    meta = utils.BuildMetadata(
        ds,
        '/dev/sda',
        swap_type='boot_disk',
        enable_dmcrypt=True,
        instance_size_label='n4-highmem-32',
    )
    self.assertEqual(meta['instance_size'], 'n4-highmem-32')

  def test_benchmark_name_stored(self):
    ds = self._basic_ds()
    meta = utils.BuildMetadata(
        ds,
        '/dev/sda',
        swap_type='boot_disk',
        enable_dmcrypt=True,
        benchmark_name='swap_encryption',
    )
    self.assertEqual(meta['benchmark'], 'swap_encryption')

  def test_flag_values_stored_in_metadata(self):
    """Flag values (swap_type, enable_dmcrypt) are recorded in metadata."""
    ds = self._basic_ds()
    meta = utils.BuildMetadata(
        ds,
        '/dev/sda',
        swap_type='hyperdisk',
        enable_dmcrypt=True,
        node_image_type='UBUNTU_CONTAINERD',
        boot_disk_type='hyperdisk-balanced',
        boot_disk_iops=80000,
        enable_zswap=False,
        min_free_kbytes=65536,
        fio_runtime_sec=60,
        nodepool='benchmark',
    )
    self.assertEqual(meta['node_image_type'], 'UBUNTU_CONTAINERD')
    self.assertEqual(meta['boot_disk_type'], 'hyperdisk-balanced')
    self.assertEqual(meta['boot_disk_iops_target'], 80000)
    self.assertEqual(meta['zswap_enabled'], False)
    self.assertEqual(meta['min_free_kbytes'], 65536)
    self.assertEqual(meta['fio_runtime_sec'], 60)
    self.assertEqual(meta['nodepool'], 'benchmark')

  def test_cloud_value_from_cloud_attribute(self):
    ds = self._basic_ds()
    ds.CLOUD = 'aws'
    meta = utils.BuildMetadata(
        ds,
        '/dev/nvme1n1',
        swap_type='instance_store',
        enable_dmcrypt=False,
    )
    self.assertEqual(meta['cloud'], 'aws')


class CollectCostSampleTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for swap_encryption_utils.CollectCostSample()."""

  _BASE_META = {
      'benchmark': 'swap_encryption',
      'swap_device': '/dev/mapper/swap_encrypted',
  }

  def test_known_gcp_instance_emits_one_sample(self):
    ds = _make_daemonset()
    samples = utils.CollectCostSample(
        ds, 3600.0, self._BASE_META, benchmark_machine_type='n4-highmem-32'
    )
    self.assertLen(samples, 1)
    self.assertEqual(samples[0].metric, 'cost_estimate_usd')

  def test_cost_value_correct(self):
    ds = _make_daemonset()
    samples = utils.CollectCostSample(
        ds, 3600.0, self._BASE_META, benchmark_machine_type='n4-highmem-32'
    )
    # n4-highmem-32 = $3.0256/hr; 1 hour → $3.0256
    self.assertAlmostEqual(samples[0].value, 3.0256, places=4)

  def test_unknown_instance_returns_empty(self):
    ds = _make_daemonset()
    samples = utils.CollectCostSample(ds, 3600.0, self._BASE_META)
    self.assertEmpty(samples)

  def test_explicit_label_overrides_benchmark_machine_type(self):
    ds = _make_daemonset()
    # Explicit label takes precedence.
    samples = utils.CollectCostSample(
        ds,
        3600.0,
        self._BASE_META,
        benchmark_machine_type='c4-standard-8',
        instance_size_label='n4-highmem-32',
    )
    self.assertAlmostEqual(samples[0].value, 3.0256, places=4)

  def test_benchmark_machine_type_fallback(self):
    ds = _make_daemonset({'metadata.google.internal': ('', '')})
    samples = utils.CollectCostSample(
        ds,
        3600.0,
        self._BASE_META,
        benchmark_machine_type='n4-highmem-32',
    )
    self.assertLen(samples, 1)
    self.assertAlmostEqual(samples[0].value, 3.0256, places=4)

  def test_sample_metadata_contains_instance_type(self):
    ds = _make_daemonset()
    samples = utils.CollectCostSample(
        ds, 1800.0, self._BASE_META, benchmark_machine_type='n4-highmem-32'
    )
    self.assertEqual(samples[0].metadata['instance_type'], 'n4-highmem-32')

  def test_sample_metadata_contains_price(self):
    ds = _make_daemonset()
    samples = utils.CollectCostSample(
        ds, 1800.0, self._BASE_META, benchmark_machine_type='n4-highmem-32'
    )
    self.assertAlmostEqual(
        samples[0].metadata['price_usd_per_hr'], 3.0256, places=4
    )

  def test_aws_instance_type_detected(self):
    ds = _make_daemonset()
    samples = utils.CollectCostSample(
        ds, 3600.0, self._BASE_META, benchmark_machine_type='i4i.4xlarge'
    )
    self.assertLen(samples, 1)
    self.assertAlmostEqual(samples[0].value, 1.4960, places=4)

  def test_half_hour_cost_correct(self):
    ds = _make_daemonset()
    samples = utils.CollectCostSample(
        ds, 1800.0, self._BASE_META, benchmark_machine_type='n4-highmem-32'
    )
    # 0.5 hours × $3.0256/hr = $1.5128
    self.assertAlmostEqual(samples[0].value, 1.5128, places=4)


if __name__ == '__main__':
  unittest.main()
