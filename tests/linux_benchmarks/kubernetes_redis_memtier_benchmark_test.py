import collections
import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker.linux_benchmarks import kubernetes_redis_memtier_benchmark
from tests import pkb_common_test_case


class KubernetesRedisMemtierBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase
):

  def test__CreateRunConfigMatrix(self):
    cls = collections.namedtuple('cls', ['field_a', 'field_b', 'field_c'])
    run_configs = kubernetes_redis_memtier_benchmark._CreateRunConfigMatrix(
        cls,
        field_a=[1, 2],
        field_b=['x', 'y'],
        field_c=[True, False],
    )
    self.assertLen(run_configs, 2**3)
    self.assertIsInstance(run_configs[0], cls)
    print(run_configs)
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
    config = kubernetes_redis_memtier_benchmark.GetConfig({})
    self.assertIsInstance(config, dict)

  def test_get_config_has_container_cluster(self):
    config = kubernetes_redis_memtier_benchmark.GetConfig({})
    self.assertIn('container_cluster', config)

  def test_get_config_servers_nodepool_present(self):
    config = kubernetes_redis_memtier_benchmark.GetConfig({})
    self.assertIn('servers', config['container_cluster']['nodepools'])

  def test_get_config_clients_nodepool_present(self):
    config = kubernetes_redis_memtier_benchmark.GetConfig({})
    self.assertIn('clients', config['container_cluster']['nodepools'])

  def test_get_config_default_server_machine_type(self):
    config = kubernetes_redis_memtier_benchmark.GetConfig({})
    machine_type = config['container_cluster']['nodepools']['servers'][
        'vm_spec']['GCP']['machine_type']
    self.assertEqual(machine_type, 'c4-standard-4')

  def test_get_config_no_swap_config_by_default(self):
    config = kubernetes_redis_memtier_benchmark.GetConfig({})
    server_np = config['container_cluster']['nodepools']['servers']
    self.assertNotIn('swap_config', server_np)


class GetConfigSwapEnabledTest(pkb_common_test_case.PkbCommonTestCase):
  """GetConfig() behaviour when --kubernetes_redis_memtier_swap_enabled=True."""

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_swap_config_injected_into_servers_nodepool(self):
    config = kubernetes_redis_memtier_benchmark.GetConfig({})
    self.assertIn(
        'swap_config',
        config['container_cluster']['nodepools']['servers'],
    )

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_swap_config_enabled_true(self):
    swap_cfg = kubernetes_redis_memtier_benchmark.GetConfig({})[
        'container_cluster']['nodepools']['servers']['swap_config']
    self.assertTrue(swap_cfg.get('enabled', False))

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_server_machine_type_upgraded_to_highmem(self):
    machine_type = kubernetes_redis_memtier_benchmark.GetConfig({})[
        'container_cluster']['nodepools']['servers']['vm_spec']['GCP'][
        'machine_type']
    self.assertEqual(
        machine_type, kubernetes_redis_memtier_benchmark._SWAP_MACHINE_TYPE)

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_server_boot_disk_type_is_hyperdisk(self):
    disk_type = kubernetes_redis_memtier_benchmark.GetConfig({})[
        'container_cluster']['nodepools']['servers']['vm_spec']['GCP'][
        'boot_disk_type']
    self.assertEqual(
        disk_type, kubernetes_redis_memtier_benchmark._SWAP_BOOT_DISK_TYPE)

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_clients_nodepool_unaffected_by_swap_toggle(self):
    config = kubernetes_redis_memtier_benchmark.GetConfig({})
    client_np = config['container_cluster']['nodepools']['clients']
    self.assertEqual(
        client_np['vm_spec']['GCP']['machine_type'], 'c4-standard-32')
    self.assertNotIn('swap_config', client_np)

  @flagsaver.flagsaver(
      kubernetes_redis_memtier_swap_enabled=True,
      kubernetes_redis_memtier_swap_swappiness=60,
  )
  def test_swap_config_swappiness_reflects_flag(self):
    swap_cfg = kubernetes_redis_memtier_benchmark.GetConfig({})[
        'container_cluster']['nodepools']['servers']['swap_config']
    self.assertEqual(swap_cfg['swappiness'], 60)

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_swap_config_has_disk_iops_and_throughput(self):
    swap_cfg = kubernetes_redis_memtier_benchmark.GetConfig({})[
        'container_cluster']['nodepools']['servers']['swap_config']
    self.assertIn('boot_disk_iops', swap_cfg)
    self.assertIn('boot_disk_throughput', swap_cfg)
    self.assertEqual(
        swap_cfg['boot_disk_iops'],
        kubernetes_redis_memtier_benchmark._SWAP_BOOT_DISK_IOPS,
    )

  @flagsaver.flagsaver(
      kubernetes_redis_memtier_swap_enabled=True,
      redis_memtier_server_machine_type='n2-highmem-16',
  )
  def test_redis_memtier_server_machine_type_wins_over_swap_default(self):
    # --redis_memtier_server_machine_type takes precedence over the swap
    # default machine type.
    machine_type = kubernetes_redis_memtier_benchmark.GetConfig({})[
        'container_cluster']['nodepools']['servers']['vm_spec']['GCP'][
        'machine_type']
    self.assertEqual(machine_type, 'n2-highmem-16')


class SwapMetadataTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _SwapMetadata() helper."""

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_swap_metadata_swap_enabled_true(self):
    self.assertTrue(
        kubernetes_redis_memtier_benchmark._SwapMetadata()['swap_enabled'])

  @flagsaver.flagsaver(
      kubernetes_redis_memtier_swap_enabled=True,
      kubernetes_redis_memtier_swap_swappiness=80,
  )
  def test_swap_metadata_swappiness_reflects_flag(self):
    self.assertEqual(
        kubernetes_redis_memtier_benchmark._SwapMetadata()['swap_swappiness'],
        80,
    )

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_swap_metadata_includes_all_expected_keys(self):
    meta = kubernetes_redis_memtier_benchmark._SwapMetadata()
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
  def test_swap_metadata_machine_type_matches_constant(self):
    self.assertEqual(
        kubernetes_redis_memtier_benchmark._SwapMetadata()['swap_machine_type'],
        kubernetes_redis_memtier_benchmark._SWAP_MACHINE_TYPE,
    )


class RunSwapMetadataTest(pkb_common_test_case.PkbCommonTestCase):
  """Verify that Run() attaches swap metadata when the flag is set."""

  def _make_sample(self):
    from perfkitbenchmarker import sample as sample_lib
    return sample_lib.Sample('throughput', 100.0, 'ops/s', {})

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=True)
  def test_run_attaches_swap_metadata_to_each_sample(self):
    fake_samples = [self._make_sample(), self._make_sample()]
    with mock.patch.object(
        kubernetes_redis_memtier_benchmark,
        '_RunMemtier',
        return_value=fake_samples,
    ):
      result = kubernetes_redis_memtier_benchmark.Run(mock.Mock())

    for s in result:
      self.assertIn('swap_enabled', s.metadata)
      self.assertTrue(s.metadata['swap_enabled'])
      self.assertIn('swap_machine_type', s.metadata)

  @flagsaver.flagsaver(kubernetes_redis_memtier_swap_enabled=False)
  def test_run_no_swap_metadata_when_flag_false(self):
    fake_samples = [self._make_sample()]
    with mock.patch.object(
        kubernetes_redis_memtier_benchmark,
        '_RunMemtier',
        return_value=fake_samples,
    ):
      result = kubernetes_redis_memtier_benchmark.Run(mock.Mock())

    for s in result:
      self.assertNotIn('swap_enabled', s.metadata)


if __name__ == '__main__':
  unittest.main()
