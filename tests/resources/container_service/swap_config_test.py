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
"""Tests for swap_config, gcp_swap_config, and aws_swap_config resources."""

import os
import unittest
from unittest import mock

from perfkitbenchmarker.providers.aws import aws_swap_config
from perfkitbenchmarker.providers.gcp import gcp_swap_config
from perfkitbenchmarker.resources.container_service import swap_config
from tests import pkb_common_test_case


class BaseSwapConfigTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the abstract BaseSwapConfig class."""

  def test_default_attrs(self):
    cfg = swap_config.BaseSwapConfig()
    self.assertEqual(cfg.swappiness, 100)
    self.assertEqual(cfg.min_free_kbytes, 67584)
    self.assertEqual(cfg.watermark_scale_factor, 500)

  def test_custom_attrs(self):
    cfg = swap_config.BaseSwapConfig(
        swappiness=60, min_free_kbytes=400, watermark_scale_factor=200
    )
    self.assertEqual(cfg.swappiness, 60)
    self.assertEqual(cfg.min_free_kbytes, 400)
    self.assertEqual(cfg.watermark_scale_factor, 200)

  def test_from_spec_raises_not_implemented(self):
    with self.assertRaises(NotImplementedError):
      swap_config.BaseSwapConfig.from_spec(mock.Mock())

  def test_create_is_noop(self):
    cfg = swap_config.BaseSwapConfig()
    cfg._Create()

  def test_delete_is_noop(self):
    cfg = swap_config.BaseSwapConfig()
    cfg._Delete()

  def test_get_swap_config_class(self):
    self.assertEqual(
        swap_config.GetSwapConfigClass('GCP'), gcp_swap_config.GkeSwapConfig
    )
    self.assertEqual(
        swap_config.GetSwapConfigClass('AWS'), aws_swap_config.EksSwapConfig
    )


class GkeSwapConfigTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for GkeSwapConfig: YAML generation, Hyperdisk validation, lifecycle."""

  def _make_spec(self, **kwargs):
    spec = mock.Mock()
    spec.swappiness = kwargs.get('swappiness', 100)
    spec.min_free_kbytes = kwargs.get('min_free_kbytes', 67584)
    spec.watermark_scale_factor = kwargs.get('watermark_scale_factor', 500)
    spec.lssd = kwargs.get('lssd', False)
    spec.lssd_count = kwargs.get('lssd_count', 0)
    spec.boot_disk_iops = kwargs.get('boot_disk_iops', 0)
    spec.boot_disk_throughput = kwargs.get('boot_disk_throughput', 0)
    return spec

  def test_from_spec_maps_all_attrs(self):
    spec = self._make_spec(
        swappiness=60,
        min_free_kbytes=400,
        watermark_scale_factor=200,
        lssd=True,
        lssd_count=2,
        boot_disk_iops=160000,
        boot_disk_throughput=2400,
    )
    cfg = gcp_swap_config.GkeSwapConfig.from_spec(spec)
    self.assertEqual(cfg.swappiness, 60)
    self.assertEqual(cfg.min_free_kbytes, 400)
    self.assertEqual(cfg.watermark_scale_factor, 200)
    self.assertTrue(cfg.lssd)
    self.assertEqual(cfg.lssd_count, 2)
    self.assertEqual(cfg.boot_disk_iops, 160000)
    self.assertEqual(cfg.boot_disk_throughput, 2400)

  def test_write_linux_config_yaml_basic_content(self):
    cfg = gcp_swap_config.GkeSwapConfig(
        swappiness=80, min_free_kbytes=300, watermark_scale_factor=400
    )
    path = cfg.WriteLinuxConfigYaml()
    try:
      with open(path) as f:
        content = f.read()
      self.assertIn('linuxConfig:', content)
      self.assertIn('swapConfig:', content)
      self.assertIn('enabled: true', content)
      self.assertIn('vm.swappiness: "80"', content)
      self.assertIn('vm.min_free_kbytes: "300"', content)
      self.assertIn('vm.watermark_scale_factor: "400"', content)
    finally:
      cfg.CleanupYaml()

  def test_write_linux_config_yaml_no_lssd_has_no_disk_profile(self):
    cfg = gcp_swap_config.GkeSwapConfig(lssd=False)
    path = cfg.WriteLinuxConfigYaml()
    try:
      with open(path) as f:
        content = f.read()
      self.assertNotIn('dedicatedLocalSsdProfile', content)
      self.assertNotIn('diskCount', content)
    finally:
      cfg.CleanupYaml()

  def test_write_linux_config_yaml_lssd_includes_disk_profile(self):
    cfg = gcp_swap_config.GkeSwapConfig(lssd=True, lssd_count=2)
    path = cfg.WriteLinuxConfigYaml()
    try:
      with open(path) as f:
        content = f.read()
      self.assertIn('dedicatedLocalSsdProfile:', content)
      self.assertIn('diskCount: 2', content)
    finally:
      cfg.CleanupYaml()

  def test_write_linux_config_yaml_returns_existing_file_path(self):
    cfg = gcp_swap_config.GkeSwapConfig()
    path = cfg.WriteLinuxConfigYaml()
    try:
      self.assertTrue(os.path.isfile(path))
    finally:
      cfg.CleanupYaml()

  def test_cleanup_yaml_removes_tempfile(self):
    cfg = gcp_swap_config.GkeSwapConfig()
    path = cfg.WriteLinuxConfigYaml()
    self.assertTrue(os.path.exists(path))
    cfg.CleanupYaml()
    self.assertFalse(os.path.exists(path))

  def test_cleanup_yaml_noop_before_write(self):
    cfg = gcp_swap_config.GkeSwapConfig()
    cfg.CleanupYaml()

  def test_cleanup_yaml_noop_on_second_call(self):
    cfg = gcp_swap_config.GkeSwapConfig()
    cfg.WriteLinuxConfigYaml()
    cfg.CleanupYaml()
    cfg.CleanupYaml()

  def test_valid_hyperdisk_throughput_no_raise_needed(self):
    cfg = gcp_swap_config.GkeSwapConfig(
        boot_disk_iops=160000, boot_disk_throughput=2400
    )
    self.assertEqual(cfg.ValidHyperdiskThroughput(), 2400)

  def test_valid_hyperdisk_throughput_raises_when_too_low(self):
    # min = ceil(160000/256) = 625; 100 < 625 -> ValueError
    cfg = gcp_swap_config.GkeSwapConfig(
        boot_disk_iops=160000, boot_disk_throughput=100
    )
    with self.assertRaises(ValueError):
      cfg.ValidHyperdiskThroughput()

  def test_valid_hyperdisk_throughput_no_iops_returns_throughput(self):
    cfg = gcp_swap_config.GkeSwapConfig(
        boot_disk_iops=0, boot_disk_throughput=500
    )
    self.assertEqual(cfg.ValidHyperdiskThroughput(), 500)

  def test_valid_hyperdisk_throughput_both_zero_returns_zero(self):
    cfg = gcp_swap_config.GkeSwapConfig(
        boot_disk_iops=0, boot_disk_throughput=0
    )
    self.assertEqual(cfg.ValidHyperdiskThroughput(), 0)

  def test_valid_hyperdisk_throughput_exact_minimum_no_raise(self):
    # iops=256, throughput=1 -> min=1; at boundary -> no raise
    cfg = gcp_swap_config.GkeSwapConfig(
        boot_disk_iops=256, boot_disk_throughput=1
    )
    self.assertEqual(cfg.ValidHyperdiskThroughput(), 1)


class EksSwapConfigTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for EksSwapConfig: nodeadm YAML output and from_spec mapping."""

  def _make_spec(self, **kwargs):
    spec = mock.Mock()
    spec.swappiness = kwargs.get('swappiness', 100)
    spec.min_free_kbytes = kwargs.get('min_free_kbytes', 67584)
    spec.watermark_scale_factor = kwargs.get('watermark_scale_factor', 500)
    return spec

  def test_from_spec_maps_sysctl_attrs(self):
    spec = self._make_spec(
        swappiness=60, min_free_kbytes=400, watermark_scale_factor=200
    )
    cfg = aws_swap_config.EksSwapConfig.from_spec(spec)
    self.assertEqual(cfg.swappiness, 60)
    self.assertEqual(cfg.min_free_kbytes, 400)
    self.assertEqual(cfg.watermark_scale_factor, 200)

  def test_from_spec_eks_specific_attrs_use_defaults(self):
    cfg = aws_swap_config.EksSwapConfig.from_spec(self._make_spec())
    self.assertEqual(cfg.memory_swap_behavior, 'LimitedSwap')
    self.assertFalse(cfg.fail_swap_on)

  def test_get_nodeadm_config_api_version(self):
    cfg = aws_swap_config.EksSwapConfig()
    self.assertIn('apiVersion: node.eks.aws/v1alpha1', cfg.GetNodeadmConfig())

  def test_get_nodeadm_config_memory_swap_behavior(self):
    cfg = aws_swap_config.EksSwapConfig()
    self.assertIn('memorySwapBehavior: LimitedSwap', cfg.GetNodeadmConfig())

  def test_get_nodeadm_config_fail_swap_on_false(self):
    cfg = aws_swap_config.EksSwapConfig(fail_swap_on=False)
    self.assertIn('failSwapOn: false', cfg.GetNodeadmConfig())

  def test_create_raises_not_implemented(self):
    # EKS impl deferred - must raise, not silently pass.
    cfg = aws_swap_config.EksSwapConfig()
    with self.assertRaises(NotImplementedError):
      cfg._Create()


if __name__ == '__main__':
  unittest.main()
