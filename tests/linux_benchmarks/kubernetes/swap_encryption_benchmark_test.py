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
"""Tests for swap_encryption_benchmark and swap_daemonset helpers.

After the Zac review refactor:
  - _ParseCipher / _DetectSwapDevice / GetResourceMetadata live in
    swap_daemonset, not in the benchmark module.
  - The benchmark module exposes only GetConfig / Prepare / Run / Cleanup.
"""

import unittest
from unittest import mock

from perfkitbenchmarker.linux_benchmarks.kubernetes import swap_encryption_benchmark
from perfkitbenchmarker.resources.container_service import swap_daemonset
from tests import pkb_common_test_case


def _make_daemonset():
  """Return a SwapDaemonSet with dummy args suitable for unit tests."""
  return swap_daemonset.SwapDaemonSet(
      name='test-ds',
      namespace='default',
      label='app=test',
      nodepool='benchmark',
  )


class GetConfigTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests that BENCHMARK_CONFIG is well-formed and loadable."""

  def test_get_config_returns_dict(self):
    config = swap_encryption_benchmark.GetConfig({})
    self.assertIsInstance(config, dict)

  def test_get_config_has_container_cluster(self):
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
  """Tests for swap_daemonset._ParseCipher() output parsing."""

  def test_parse_cipher_standard_aes_xts(self):
    status = '0 67108864 crypt aes-xts-plain64 0 8:16 0 1 sector_size:4096'
    self.assertEqual(swap_daemonset._ParseCipher(status), 'aes-xts-plain64')

  def test_parse_cipher_returns_empty_when_no_crypt_token(self):
    status = '0 67108864 linear 8:16 0'
    self.assertEqual(swap_daemonset._ParseCipher(status), '')

  def test_parse_cipher_returns_empty_on_empty_string(self):
    self.assertEqual(swap_daemonset._ParseCipher(''), '')

  def test_parse_cipher_crypt_at_end_returns_empty(self):
    status = 'something crypt'
    self.assertEqual(swap_daemonset._ParseCipher(status), '')

  def test_parse_cipher_not_encrypted_string(self):
    status = 'not_encrypted'
    self.assertEqual(swap_daemonset._ParseCipher(status), '')


class DetectSwapDeviceTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for SwapDaemonSet._DetectSwapDevice()."""

  def test_detect_swap_device_returns_device_basename(self):
    ds = _make_daemonset()
    ds.PodExec = mock.Mock(return_value=('/dev/dm-0\n', ''))
    self.assertEqual(ds._DetectSwapDevice(), 'dm-0')

  def test_detect_swap_device_returns_first_device_when_multiple(self):
    ds = _make_daemonset()
    ds.PodExec = mock.Mock(return_value=('/dev/dm-0\n/dev/dm-1\n', ''))
    self.assertEqual(ds._DetectSwapDevice(), 'dm-0')

  def test_detect_swap_device_returns_empty_when_no_swap(self):
    ds = _make_daemonset()
    ds.PodExec = mock.Mock(return_value=('', ''))
    self.assertEqual(ds._DetectSwapDevice(), '')

  def test_detect_swap_device_raises_on_pod_exec_failure(self):
    # Per Zac review: fail fast - no silent swallowing.
    ds = _make_daemonset()
    ds.PodExec = mock.Mock(side_effect=Exception('pod not found'))
    with self.assertRaises(Exception):
      ds._DetectSwapDevice()


class GetResourceMetadataTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for SwapDaemonSet.GetResourceMetadata()."""

  def _make_ds_with_responses(
      self,
      swap_path='/dev/dm-0',
      kver='5.15.0-gke-1234',
      dmstatus='0 67108864 crypt aes-xts-plain64 0 8:16 0',
  ):
    ds = _make_daemonset()

    def _pod_exec(cmd, **_):
      if 'proc/swaps' in cmd:
        return (swap_path + '\n' if swap_path else '', '')
      if 'uname' in cmd:
        return (kver + '\n', '')
      if 'dmsetup' in cmd:
        return (dmstatus, '')
      return ('', '')

    ds.PodExec = mock.Mock(side_effect=_pod_exec)
    return ds

  def test_metadata_includes_swap_device(self):
    ds = self._make_ds_with_responses()
    meta = ds.GetResourceMetadata()
    self.assertEqual(meta['swap_device'], 'dm-0')

  def test_metadata_swap_device_unknown_when_no_swap(self):
    ds = self._make_ds_with_responses(swap_path='')
    meta = ds.GetResourceMetadata()
    self.assertEqual(meta['swap_device'], 'unknown')

  def test_metadata_includes_kernel_version(self):
    ds = self._make_ds_with_responses()
    meta = ds.GetResourceMetadata()
    self.assertEqual(meta['kernel_version'], '5.15.0-gke-1234')

  def test_metadata_includes_swap_cipher(self):
    ds = self._make_ds_with_responses()
    meta = ds.GetResourceMetadata()
    self.assertEqual(meta['swap_cipher'], 'aes-xts-plain64')

  def test_metadata_raises_on_pod_exec_failure(self):
    # Per Zac review: fail fast - no silent swallowing.
    ds = _make_daemonset()
    ds.PodExec = mock.Mock(side_effect=Exception('timeout'))
    with self.assertRaises(Exception):
      ds.GetResourceMetadata()


if __name__ == '__main__':
  unittest.main()
