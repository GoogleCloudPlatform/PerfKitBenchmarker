# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for kubernetes_redis_memtier_benchmark."""

# Tests intentionally access private module symbols (_SWAP_*, _SwapMetadata,
# _CreateRunConfigMatrix) to verify internal behaviour without a full cluster.
# pylint: disable=protected-access,invalid-name

import collections
# pylint: disable=missing-function-docstring,too-few-public-methods
import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker import sample as sample_lib
from perfkitbenchmarker.linux_benchmarks import (
    kubernetes_redis_memtier_benchmark as benchmark
)
from tests import pkb_common_test_case


class KubernetesRedisMemtierBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase
):
  """Tests for the _CreateRunConfigMatrix helper."""

  def test__CreateRunConfigMatrix(self):
    cls = collections.namedtuple('cls', ['field_a', 'field_b', 'field_c'])
    run_configs = benchmark._CreateRunConfigMatrix(
        cls,
        field_a=[1, 2],
        field_b=['x', 'y'],
        field_c=[True, False],
    )
    self.assertLen(run_configs, 2**3)
    self.assertIsInstance(run_configs[0], cls)
    self.assertEqual(
        run_configs,
        [
            cls(field_a=1, field_b='x', field_c=True),
            cls(field_a=1, field_b='x', field_c=False),
            cls(field_a=1, field_b='y', field_c=True),
            cls(field_a=1, field_b='y', field_c=False),
            cls(field_a=2, field_b='x', field_c=True),
            cls(field_a=2, field_b='x', field_c=False),
            cls(field_a=2, field_b='y', field_c=True),
            cls(field_a=2, field_b='y', field_c=False),
        ],
    )


class GetConfigNoSwapTest(pkb_common_test_case.PkbCommonTestCase):
  """GetConfig() default behaviour -- swap flag is False."""

  def test_get_config_returns_dict(self):
    self.assertIsInstance(benchmark.GetConfig({}), dict)

  def test_get_config_has_container_cluster(self):
    self.assertIn('container_cluster', benchmark.GetConfig({}))

  def test_get_config_servers_nodepool_present(self):
    config = benchmark.GetConfig({})
    self.assertIn('servers', config['container_cluster']['nodepools'])

  def test_get_config_clients_nodepool_present(self):
    config = benchmark.GetConfig({})
    self.assertIn('clients', config['container_cluster']['nodepools'])

  def test_get_config_gcp_default_server_machine_type(self):
    config = benchmark.GetConfig({})
    machine_type = (
        config['container_cluster']['nodepools']['servers']
        ['vm_spec']['GCP']['machine_type']
    )
    self.assertEqual(machine_type, 'c4-standard-4')

  def test_get_config_aws_default_server_machine_type(self):
    config = benchmark.GetConfig({})
    machine_type = (
        config['container_cluster']['nodepools']['servers']
        ['vm_spec']['AWS']['machine_type']
    )
    self.assertEqual(machine_type, 'm5.xlarge')

  def test_get_config_no_swap_config_by_default(self):
    server_np = (
        benchmark.GetConfig({})['container_cluster']['nodepools']['servers']
    )
    self.assertNotIn('swap_config', server_np)


class GetConfigSwapEnabledGcpTest(pkb_common_test_case.PkbCommonTestCase):
  """GetConfig() GCP behaviour when --kubernetes_redis_memtier_swap_enabled."""

  def _server_np(self):
    return (
        benchmark.GetConfig({})['container_cluster']['nodepools']['servers']
    )

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_swap_config_injected_into_servers_nodepool(self):
    self.assertIn('swap_config', self._server_np())

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_swap_config_enabled_true(self):
    self.assertTrue(self._server_np()['swap_config'].get('enabled', False))

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_server_machine_type_upgraded_to_highmem(self):
    machine_type = self._server_np()['vm_spec']['GCP']['machine_type']
    self.assertEqual(machine_type, benchmark._SWAP_MACHINE_TYPE)

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_server_boot_disk_type_is_hyperdisk(self):
    disk_type = self._server_np()['vm_spec']['GCP']['boot_disk_type']
    self.assertEqual(disk_type, benchmark._SWAP_BOOT_DISK_TYPE)

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_clients_nodepool_unaffected_by_swap_toggle(self):
    config = benchmark.GetConfig({})
    client_np = config['container_cluster']['nodepools']['clients']
    self.assertEqual(
        client_np['vm_spec']['GCP']['machine_type'], 'c4-standard-32')
    self.assertNotIn('swap_config', client_np)

  @flagsaver.flagsaver(
      kubernetes_redis_memtier_swap_enabled=True,
      kubernetes_redis_memtier_swap_swappiness=60,
  )
  def test_swap_config_swappiness_reflects_flag(self):
    self.assertEqual(self._server_np()['swap_config']['swappiness'], 60)

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_swap_config_has_gcp_disk_iops_and_throughput(self):
    swap_cfg = self._server_np()['swap_config']
    self.assertEqual(swap_cfg['boot_disk_iops'], benchmark._SWAP_BOOT_DISK_IOPS)
    self.assertEqual(
        swap_cfg['boot_disk_throughput'], benchmark._SWAP_BOOT_DISK_THROUGHPUT)

  @flagsaver.flagsaver(
      kubernetes_redis_memtier_swap_enabled=True,
      redis_memtier_server_machine_type='n2-highmem-16',
  )
  def test_redis_memtier_server_machine_type_wins_over_swap_default(self):
    # --redis_memtier_server_machine_type takes precedence over the swap
    # default machine type.
    machine_type = self._server_np()['vm_spec']['GCP']['machine_type']
    self.assertEqual(machine_type, 'n2-highmem-16')


class GetConfigSwapEnabledAwsTest(pkb_common_test_case.PkbCommonTestCase):
  """GetConfig() AWS behaviour when --kubernetes_redis_memtier_swap_enabled."""

  def _server_np(self):
    return (
        benchmark.GetConfig({})['container_cluster']['nodepools']['servers']
    )

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_aws_server_machine_type_upgraded_to_r6i(self):
    machine_type = self._server_np()['vm_spec']['AWS']['machine_type']
    self.assertEqual(machine_type, benchmark._SWAP_AWS_MACHINE_TYPE)

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_aws_server_boot_disk_type_is_gp3(self):
    disk_type = self._server_np()['vm_spec']['AWS']['boot_disk_type']
    self.assertEqual(disk_type, benchmark._SWAP_AWS_BOOT_DISK_TYPE)

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_aws_server_boot_disk_size(self):
    disk_size = self._server_np()['vm_spec']['AWS']['boot_disk_size']
    self.assertEqual(disk_size, benchmark._SWAP_AWS_BOOT_DISK_SIZE)

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_swap_config_has_iops_and_throughput(self):
    swap_cfg = self._server_np()['swap_config']
    self.assertIn('boot_disk_iops', swap_cfg)
    self.assertIn('boot_disk_throughput', swap_cfg)

  @flagsaver.flagsaver(
      kubernetes_redis_memtier_swap_enabled=True,
      redis_memtier_server_machine_type='r7i.8xlarge',
  )
  def test_aws_server_machine_type_flag_wins_over_swap_default(self):
    machine_type = self._server_np()['vm_spec']['AWS']['machine_type']
    self.assertEqual(machine_type, 'r7i.8xlarge')

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_gcp_entry_unaffected_by_aws_swap_constants(self):
    # GCP vm_spec should still get hyperdisk-balanced, not gp3.
    disk_type = self._server_np()['vm_spec']['GCP']['boot_disk_type']
    self.assertEqual(disk_type, benchmark._SWAP_BOOT_DISK_TYPE)


class SwapMetadataGcpTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _SwapMetadata() -- GCP (default cloud)."""

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_swap_metadata_swap_enabled_true(self):
    self.assertTrue(benchmark._SwapMetadata()['swap_enabled'])

  @flagsaver.flagsaver(
      kubernetes_redis_memtier_swap_enabled=True,
      kubernetes_redis_memtier_swap_swappiness=80,
  )
  def test_swap_metadata_swappiness_reflects_flag(self):
    self.assertEqual(benchmark._SwapMetadata()['swap_swappiness'], 80)

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_swap_metadata_includes_all_expected_keys(self):
    """All metadata keys required for result analysis must be present."""
    meta = benchmark._SwapMetadata()
    for key in (
        'swap_machine_type',
        'swap_boot_disk_type',
        'swap_boot_disk_size_gb',
        'swap_boot_disk_iops',
        'swap_boot_disk_throughput',
        'swap_min_free_kbytes',
        'swap_watermark_scale_factor',
    ):
      self.assertIn(key, meta, msg=f'Missing key: {key}')

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_swap_metadata_gcp_machine_type_by_default(self):
    self.assertEqual(
        benchmark._SwapMetadata()['swap_machine_type'],
        benchmark._SWAP_MACHINE_TYPE,
    )

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_swap_metadata_gcp_disk_type_by_default(self):
    self.assertEqual(
        benchmark._SwapMetadata()['swap_boot_disk_type'],
        benchmark._SWAP_BOOT_DISK_TYPE,
    )


class SwapMetadataAwsTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _SwapMetadata() -- AWS."""

  @flagsaver.flagsaver(
      kubernetes_redis_memtier_swap_enabled=True,
      cloud='AWS',
  )
  def test_swap_metadata_aws_machine_type(self):
    self.assertEqual(
        benchmark._SwapMetadata()['swap_machine_type'],
        benchmark._SWAP_AWS_MACHINE_TYPE,
    )

  @flagsaver.flagsaver(
      kubernetes_redis_memtier_swap_enabled=True,
      cloud='AWS',
  )
  def test_swap_metadata_aws_disk_type_is_gp3(self):
    self.assertEqual(
        benchmark._SwapMetadata()['swap_boot_disk_type'],
        benchmark._SWAP_AWS_BOOT_DISK_TYPE,
    )

  @flagsaver.flagsaver(
      kubernetes_redis_memtier_swap_enabled=True,
      cloud='AWS',
  )
  def test_swap_metadata_aws_iops(self):
    self.assertEqual(
        benchmark._SwapMetadata()['swap_boot_disk_iops'],
        benchmark._SWAP_AWS_BOOT_DISK_IOPS,
    )


class RunSwapMetadataTest(pkb_common_test_case.PkbCommonTestCase):
  """Verify that Run() attaches swap metadata when the flag is set."""

  def _make_sample(self):
    """Return a minimal Sample for use in Run() mock tests."""
    return sample_lib.Sample('throughput', 100.0, 'ops/s', {})

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_run_attaches_swap_metadata_to_each_sample(self):
    fake_samples = [self._make_sample(), self._make_sample()]
    with mock.patch.object(
        benchmark, '_RunMemtier', return_value=fake_samples,
    ):
      result = benchmark.Run(mock.Mock())

    for s in result:
      self.assertIn('swap_enabled', s.metadata)
      self.assertTrue(s.metadata['swap_enabled'])
      self.assertIn('swap_machine_type', s.metadata)

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=False)
  def test_run_no_swap_metadata_when_flag_false(self):
    fake_samples = [self._make_sample()]
    with mock.patch.object(
        benchmark, '_RunMemtier', return_value=fake_samples,
    ):
      result = benchmark.Run(mock.Mock())

    for s in result:
      self.assertNotIn('swap_enabled', s.metadata)



class PrepareSwapTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests that Prepare() registers the SwapDaemonSet in spec.resources."""

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch.object(benchmark, '_swap_daemonset_lib')
    )
    # Prevent Prepare() from calling actual kubectl / redis setup.
    self.enter_context(
        mock.patch.object(benchmark, 'kubernetes_commands')
    )

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_prepare_adds_daemonset_to_spec_resources(self):
    """SwapDaemonSet must be appended to spec.resources before Create()."""
    spec = mock.Mock()
    spec.resources = []
    ds_instance = mock.Mock()
    benchmark._swap_daemonset_lib.SwapDaemonSet.return_value = ds_instance

    benchmark.Prepare(spec)

    # DaemonSet was registered in spec.resources.
    self.assertIn(ds_instance, spec.resources)

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_prepare_appends_before_create(self):
    """spec.resources.append() must be called before daemonset.Create()."""
    call_order = []
    spec = mock.Mock()
    spec.resources = mock.MagicMock()
    spec.resources.append.side_effect = lambda x: call_order.append('append')
    ds_instance = mock.Mock()
    ds_instance.Create.side_effect = lambda: call_order.append('create')
    ds_instance.WaitForPod.return_value = 'pkb-swap-pod-0'
    benchmark._swap_daemonset_lib.SwapDaemonSet.return_value = ds_instance

    benchmark.Prepare(spec)

    self.assertEqual(call_order, ['append', 'create'],
                     'spec.resources.append() must precede daemonset.Create()')

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=False)
  def test_prepare_no_daemonset_when_swap_disabled(self):
    """No SwapDaemonSet created or registered when swap flag is False."""
    spec = mock.Mock()
    spec.resources = []

    benchmark.Prepare(spec)

    benchmark._swap_daemonset_lib.SwapDaemonSet.assert_not_called()
    self.assertEmpty(spec.resources)

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_cleanup_does_not_manually_delete_daemonset(self):
    """Cleanup() must not manually call .Delete().

    PKB auto-cleanup handles it.
    """
    spec = mock.Mock()
    spec.resources = []
    ds_instance = mock.Mock()
    benchmark._swap_daemonset_lib.SwapDaemonSet.return_value = ds_instance

    benchmark.Cleanup(spec)

    ds_instance.Delete.assert_not_called()


if __name__ == '__main__':
  unittest.main()
