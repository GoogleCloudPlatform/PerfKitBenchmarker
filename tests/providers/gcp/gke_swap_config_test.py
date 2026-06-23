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
"""Tests for declarative node-pool swap to GKE managed swapConfig rendering.

Tests that a `swap_config` on the node pool produces the native
linuxConfig.swapConfig system-config file, letting GKE provision and encrypt
the swap device without privileged DaemonSets or cgroup edits.
"""

from unittest import mock

from absl.testing import absltest
from perfkitbenchmarker.configs import container_spec as container_spec_lib
from perfkitbenchmarker.providers.gcp import google_kubernetes_engine
import yaml


def _MakeSwapConfig(**kwargs):
  return container_spec_lib.SwapConfigSpec(
      'test_swap_config', flag_values=mock.Mock(), **kwargs
  )


class GkeSwapSystemConfigTest(absltest.TestCase):
  """_WriteSwapSystemConfigFile renders the documented GKE swap schema."""

  def _Render(self, swap_config, base_path=None):
    # _WriteSwapSystemConfigFile is a pure rendering method; bind it unbound so
    # the test needs no real cluster / GCP credentials.
    cluster = mock.Mock(spec=['_WriteSwapSystemConfigFile'])
    path = google_kubernetes_engine.GkeCluster._WriteSwapSystemConfigFile(
        cluster, swap_config, base_path
    )
    with open(path) as f:
      return yaml.safe_load(f)

  def testEncryptedLocalSsdIsDefaultMatrixRow(self):
    cfg = self._Render(_MakeSwapConfig(enabled=True, backing_store='local_ssd'))
    swap = cfg['linuxConfig']['swapConfig']
    self.assertTrue(swap['enabled'])
    self.assertEqual(swap['ephemeralLocalSsdProfile']['swapSizePercent'], 30)
    # Encrypted is GKE's default -> no encryptionConfig override emitted.
    self.assertNotIn('encryptionConfig', swap)
    # GKE manages swap behavior itself; no kubelet key should be injected (an
    # unknown key would make gcloud reject the whole system-config).
    self.assertNotIn('kubeletConfig', cfg)

  def testExplicitSizeGibOverridesPercent(self):
    cfg = self._Render(
        _MakeSwapConfig(enabled=True, backing_store='local_ssd', size_gb=64)
    )
    profile = cfg['linuxConfig']['swapConfig']['ephemeralLocalSsdProfile']
    self.assertEqual(profile, {'swapSizeGib': 64})

  def testUnencryptedBaselineRowDisablesEncryption(self):
    cfg = self._Render(
        _MakeSwapConfig(enabled=True, backing_store='local_ssd', encrypted=False)
    )
    swap = cfg['linuxConfig']['swapConfig']
    self.assertEqual(swap['encryptionConfig'], {'disabled': True})

  def testBootDiskProfileCarriesSize(self):
    cfg = self._Render(
        _MakeSwapConfig(
            enabled=True, backing_store='boot_disk', size_percent=20
        )
    )
    swap = cfg['linuxConfig']['swapConfig']
    self.assertEqual(swap['bootDiskProfile'], {'swapSizePercent': 20})
    self.assertNotIn('ephemeralLocalSsdProfile', swap)

  def testMergesUserSuppliedSystemConfig(self):
    base = self.create_tempfile(
        content=yaml.safe_dump({'linuxConfig': {'sysctl': {'vm.swappiness': '60'}}})
    )
    cfg = self._Render(
        _MakeSwapConfig(enabled=True, backing_store='local_ssd'),
        base_path=base.full_path,
    )
    # Both the user's sysctl and our swapConfig survive the merge.
    self.assertEqual(cfg['linuxConfig']['sysctl']['vm.swappiness'], '60')
    self.assertTrue(cfg['linuxConfig']['swapConfig']['enabled'])


if __name__ == '__main__':
  absltest.main()
