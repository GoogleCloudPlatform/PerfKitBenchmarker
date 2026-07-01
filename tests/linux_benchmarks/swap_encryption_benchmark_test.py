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
"""Tests for perfkitbenchmarker.linux_benchmarks.swap_encryption_benchmark."""

import unittest
from unittest import mock

from perfkitbenchmarker.linux_benchmarks import swap_encryption_benchmark
from tests import pkb_common_test_case


class GetConfigTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests that BENCHMARK_CONFIG is well-formed and loadable."""

  def test_get_config_returns_dict(self):
    config = swap_encryption_benchmark.GetConfig({})
    self.assertIsInstance(config, dict)

  def test_get_config_has_container_cluster(self):
    # configs.LoadConfig returns the inner benchmark dict directly (no benchmark
    # name wrapper), so top-level keys are 'container_cluster', 'description', etc.
    config = swap_encryption_benchmark.GetConfig({})
    self.assertIn('container_cluster', config)

  def test_get_config_benchmark_nodepool_present(self):
    config = swap_encryption_benchmark.GetConfig({})
    nodepools = config['container_cluster']['nodepools']
    self.assertIn(
        swap_encryption_benchmark._BENCHMARK_NODEPOOL,
        nodepools,
    )

  def test_get_config_swap_config_present_on_benchmark_nodepool(self):
    config = swap_encryption_benchmark.GetConfig({})
    nodepool = config['container_cluster']['nodepools'][
        swap_encryption_benchmark._BENCHMARK_NODEPOOL
    ]
    self.assertIn('swap_config', nodepool)
    self.assertTrue(nodepool['swap_config'].get('enabled', False))


class ParseCipherTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _parse_cipher() output parsing."""

  def test_parse_cipher_standard_aes_xts(self):
    # Typical dmsetup status line: <name> <start>-<end> crypt <cipher> ...
    status = '0 67108864 crypt aes-xts-plain64 0 8:16 0 1 sector_size:4096'
    self.assertEqual(
        swap_encryption_benchmark._parse_cipher(status), 'aes-xts-plain64'
    )

  def test_parse_cipher_returns_empty_when_no_crypt_token(self):
    status = '0 67108864 linear 8:16 0'
    self.assertEqual(swap_encryption_benchmark._parse_cipher(status), '')

  def test_parse_cipher_returns_empty_on_empty_string(self):
    self.assertEqual(swap_encryption_benchmark._parse_cipher(''), '')

  def test_parse_cipher_crypt_at_end_returns_empty(self):
    # 'crypt' present but no token after it.
    status = 'something crypt'
    self.assertEqual(swap_encryption_benchmark._parse_cipher(status), '')

  def test_parse_cipher_not_encrypted_string(self):
    # Output from the benchmark when dm-crypt not active.
    status = 'not_encrypted'
    self.assertEqual(swap_encryption_benchmark._parse_cipher(status), '')


class DetectSwapDeviceTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _detect_swap_device() with mocked PodExec."""

  def _make_ds(self, pod_exec_output):
    ds = mock.Mock()
    ds.PodExec.return_value = (pod_exec_output, '')
    return ds

  def test_detect_swap_device_returns_device_basename(self):
    # /proc/swaps first device column (after header skip via awk NR>1).
    ds = self._make_ds('/dev/dm-0\n')
    result = swap_encryption_benchmark._detect_swap_device(ds)
    self.assertEqual(result, 'dm-0')

  def test_detect_swap_device_returns_first_device_when_multiple(self):
    ds = self._make_ds('/dev/dm-0\n/dev/dm-1\n')
    result = swap_encryption_benchmark._detect_swap_device(ds)
    self.assertEqual(result, 'dm-0')

  def test_detect_swap_device_returns_empty_when_no_swap(self):
    ds = self._make_ds('')
    result = swap_encryption_benchmark._detect_swap_device(ds)
    self.assertEqual(result, '')

  def test_detect_swap_device_returns_empty_on_pod_exec_exception(self):
    ds = mock.Mock()
    ds.PodExec.side_effect = Exception('pod not found')
    result = swap_encryption_benchmark._detect_swap_device(ds)
    self.assertEqual(result, '')


class BuildMetadataTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _build_metadata() with mocked PodExec."""

  def test_build_metadata_includes_swap_device(self):
    ds = mock.Mock()
    ds.PodExec.return_value = ('5.15.0-gke-1234\n', '')
    meta = swap_encryption_benchmark._build_metadata(ds, 'dm-0')
    self.assertEqual(meta['swap_device'], 'dm-0')

  def test_build_metadata_swap_device_unknown_when_empty(self):
    ds = mock.Mock()
    ds.PodExec.return_value = ('5.15.0\n', '')
    meta = swap_encryption_benchmark._build_metadata(ds, '')
    self.assertEqual(meta['swap_device'], 'unknown')

  def test_build_metadata_includes_kernel_version(self):
    ds = mock.Mock()
    ds.PodExec.return_value = ('5.15.0-gke-1234\n', '')
    meta = swap_encryption_benchmark._build_metadata(ds, 'dm-0')
    self.assertEqual(meta['kernel_version'], '5.15.0-gke-1234')

  def test_build_metadata_kernel_version_absent_on_pod_exec_exception(self):
    ds = mock.Mock()
    ds.PodExec.side_effect = Exception('timeout')
    meta = swap_encryption_benchmark._build_metadata(ds, 'dm-0')
    self.assertNotIn('kernel_version', meta)


if __name__ == '__main__':
  unittest.main()
