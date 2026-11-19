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

import threading
import time
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
import jinja2
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.linux_benchmarks import cassandra_stress_benchmark
from perfkitbenchmarker.linux_benchmarks import cassandra_stress_lssd_benchmark
from perfkitbenchmarker.linux_benchmarks import lssd_workloads_util
from perfkitbenchmarker.linux_packages import cassandra
from tests import pkb_common_test_case
import yaml

FLAGS = flags.FLAGS


def _render_jinja_file(template_path: str, context: dict[str, object]) -> str:
  with open(template_path, encoding='utf-8') as f:
    template = jinja2.Template(f.read(), undefined=jinja2.StrictUndefined)
  return template.render(context)


class CassandraStressLssdBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase
):

  def _CreateTestVm(self, name: str, ip: str):
    vm = mock.create_autospec(
        pkb_common_test_case.TestLinuxVirtualMachine, instance=True
    )
    vm.name = name
    vm.hostname = name
    vm.internal_ip = ip
    vm.NumCpusForBenchmark.return_value = 16
    vm.GetScratchDir.return_value = '/scratch'
    return vm

  def _CreateBenchmarkSpec(self):
    server_vm_0 = self._CreateTestVm('server-0', '10.0.0.1')
    server_vm_1 = self._CreateTestVm('server-1', '10.0.0.2')
    client_vm = self._CreateTestVm('client-0', '10.0.0.3')
    spec = mock.create_autospec(bm_spec.BenchmarkSpec, instance=True)
    spec.vm_groups = {
        'workers': [server_vm_0],
        'workers_1': [server_vm_1],
        'client': [client_vm],
    }
    return spec, [server_vm_0, server_vm_1], client_vm

  def _StubPrepareCollaborators(self):
    mock_precondition = self.enter_context(
        mock.patch.object(
            lssd_workloads_util,
            'PreconditionDisk',
            autospec=True,
            spec_set=True,
        )
    )
    mock_create_fs = self.enter_context(
        mock.patch.object(
            lssd_workloads_util,
            'CreateFileSystemAndMount',
            autospec=True,
            spec_set=True,
        )
    )
    mock_configure = self.enter_context(
        mock.patch.object(
            cassandra, 'Configure', autospec=True, spec_set=True
        )
    )
    self.enter_context(
        mock.patch.object(
            cassandra, 'StartCluster', autospec=True, spec_set=True
        )
    )
    self.enter_context(
        mock.patch.object(
            cassandra, 'CreateKeyspace', autospec=True, spec_set=True
        )
    )
    self.enter_context(
        mock.patch.object(
            cassandra,
            'UpdateCassandraConcurrentCompactorCount',
            autospec=True,
            spec_set=True,
        )
    )
    mock_pending = self.enter_context(
        mock.patch.object(
            cassandra,
            'GetPendingTaskCountFromCompactionStats',
            autospec=True,
            spec_set=True,
        )
    )
    mock_pending.return_value = [0, 0]
    self.enter_context(
        mock.patch.object(
            cassandra_stress_benchmark,
            'RunCassandraStressTestOnClients',
            autospec=True,
            spec_set=True,
        )
    )
    self.enter_context(
        mock.patch.object(time, 'sleep', autospec=True, spec_set=True)
    )
    return mock_precondition, mock_create_fs, mock_configure

  def test_get_config_loads_lssd_defaults_and_constructs_spec(self):
    config = cassandra_stress_lssd_benchmark.GetConfig({})

    for group_name in ('workers', 'workers_1', 'workers_2'):
      gcp_disk_spec = config['vm_groups'][group_name]['disk_spec']['GCP']
      self.assertEqual(gcp_disk_spec['disk_type'], 'local')
      self.assertEqual(gcp_disk_spec['interface'], 'NVME')
      self.assertEqual(gcp_disk_spec['mount_point'], '/scratch')
    self.assertEqual(
        config['vm_groups']['workers']['vm_spec']['Azure']['machine_type'],
        'Standard_L16s_v3',
    )
    self.assertEqual(
        config['vm_groups']['workers']['vm_spec']['AWS']['machine_type'],
        'i4i.4xlarge',
    )
    self.assertEqual(
        config['flags']['db_machine_type'], 'z3-highmem-16-highlssd'
    )
    self.assertNotIn('client_vm_machine_type', config['flags'])
    self.assertNotIn('data_disk_type', config['flags'])
    self.assertEqual(
        config['flags']['cassandra_yaml_template_path'],
        'cassandra/cassandra-lssd.yaml.j2',
    )
    self.assertTrue(config['flags']['lssd_workload_precondition_disk'])
    self.assertTrue(config['flags']['iostat'])
    self.assertFalse(config['flags']['sar'])

    spec = benchmark_config_spec.BenchmarkConfigSpec(
        cassandra_stress_lssd_benchmark.BENCHMARK_NAME,
        flag_values=FLAGS,
        **config,
    )
    for group_name in ('workers', 'workers_1', 'workers_2'):
      self.assertEqual(
          spec.vm_groups[group_name].disk_spec.num_striped_disks, 2
      )

  @parameterized.named_parameters(
      ('single_lssd_c4d', 'c4d-highmem-16-lssd', 1),
      ('five_lssd_c4', 'c4-standard-32-lssd', 5),
      ('ten_lssd_c4a', 'c4a-highmem-48-lssd', 10),
  )
  def test_get_config_stripes_all_bundled_gcp_local_ssds(
      self, db_machine_type, expected_striped_disks
  ):
    FLAGS['db_machine_type'].parse(db_machine_type)
    config = cassandra_stress_lssd_benchmark.GetConfig({})
    spec = benchmark_config_spec.BenchmarkConfigSpec(
        cassandra_stress_lssd_benchmark.BENCHMARK_NAME,
        flag_values=FLAGS,
        **config,
    )

    for group_name in ('workers', 'workers_1', 'workers_2'):
      self.assertEqual(
          spec.vm_groups[group_name].vm_spec.machine_type,
          db_machine_type,
      )
      self.assertEqual(
          spec.vm_groups[group_name].disk_spec.num_striped_disks,
          expected_striped_disks,
      )

  def test_get_config_preserves_explicit_num_striped_disks_override(self):
    user_config = {
        'vm_groups': {
            'workers': {'disk_spec': {'GCP': {'num_striped_disks': 1}}}
        }
    }
    FLAGS['db_machine_type'].parse('c4-standard-32-lssd')
    config = cassandra_stress_lssd_benchmark.GetConfig(user_config)
    spec = benchmark_config_spec.BenchmarkConfigSpec(
        cassandra_stress_lssd_benchmark.BENCHMARK_NAME,
        flag_values=FLAGS,
        **config,
    )

    for group_name in ('workers', 'workers_1', 'workers_2'):
      self.assertEqual(
          spec.vm_groups[group_name].disk_spec.num_striped_disks,
          1,
      )

  def test_default_benchmark_config_lssd_entry_constructs_spec(self):
    default_configs = configs.GetDefaultAndUserConfig()
    user_config = default_configs['cassandra_stress_mixed_workload_lssd']

    with flag_util.OverrideFlags(FLAGS, user_config.get('flags')):
      config_dict = cassandra_stress_lssd_benchmark.GetConfig(user_config)
      spec = benchmark_config_spec.BenchmarkConfigSpec(
          cassandra_stress_lssd_benchmark.BENCHMARK_NAME,
          flag_values=FLAGS,
          **config_dict,
      )

    self.assertEqual(spec.flags['cassandra_stress_command'], 'mixed')
    self.assertEqual(
        spec.flags['cassandra_stress_mixed_ratio'], 'write=3,read=1'
    )

  @flagsaver.flagsaver(lssd_workload_precondition_disk=False)
  def test_prepare_skips_preconditioning_when_disabled(self):
    spec, server_vms, client_vm = self._CreateBenchmarkSpec()
    mock_precondition, mock_create_fs, _ = self._StubPrepareCollaborators()

    cassandra_stress_lssd_benchmark.Prepare(spec)

    mock_precondition.assert_not_called()
    mock_create_fs.assert_not_called()
    for vm in server_vms + [client_vm]:
      vm.InstallPackages.assert_called_once_with('sysstat')

  @flagsaver.flagsaver(
      lssd_workload_precondition_disk=True, disk_fs_type='ext4'
  )
  def test_prepare_preconditions_server_disks_before_configure(self):
    spec, server_vms, client_vm = self._CreateBenchmarkSpec()
    mock_precondition, mock_create_fs, mock_configure = (
        self._StubPrepareCollaborators()
    )
    lock = threading.Lock()
    events = []

    def record_precondition(vm):
      with lock:
        events.append(('precondition', vm.name))

    def record_create_fs(vm, fs_type='xfs'):
      with lock:
        events.append(('create_fs', vm.name, fs_type))

    def record_configure(vm, seed_vms, custom_cassandra_conf=None):
      del seed_vms, custom_cassandra_conf
      with lock:
        events.append(('configure', vm.name))

    mock_precondition.side_effect = record_precondition
    mock_create_fs.side_effect = record_create_fs
    mock_configure.side_effect = record_configure

    cassandra_stress_lssd_benchmark.Prepare(spec)

    self.assertCountEqual(
        [call.args[0] for call in mock_precondition.call_args_list],
        server_vms,
    )
    self.assertCountEqual(
        [call.args[0] for call in mock_create_fs.call_args_list],
        server_vms,
    )
    for server_vm in server_vms:
      server_vm.InstallPackages.assert_any_call('fio')
      precondition_idx = events.index(('precondition', server_vm.name))
      create_fs_idx = events.index(('create_fs', server_vm.name, 'ext4'))
      self.assertLess(precondition_idx, create_fs_idx)
    client_vm.InstallPackages.assert_called_once_with('sysstat')
    max_create_fs_idx = max(
        idx for idx, event in enumerate(events) if event[0] == 'create_fs'
    )
    min_configure_idx = min(
        idx for idx, event in enumerate(events) if event[0] == 'configure'
    )
    self.assertLess(max_create_fs_idx, min_configure_idx)

  def test_cleanup_stops_and_cleans_all_server_nodes(self):
    spec, server_vms, _ = self._CreateBenchmarkSpec()
    mock_stop = self.enter_context(
        mock.patch.object(cassandra, 'Stop', autospec=True, spec_set=True)
    )
    mock_clean = self.enter_context(
        mock.patch.object(
            cassandra, 'CleanNode', autospec=True, spec_set=True
        )
    )

    cassandra_stress_lssd_benchmark.Cleanup(spec)

    self.assertCountEqual(
        [call.args[0] for call in mock_stop.call_args_list], server_vms
    )
    self.assertCountEqual(
        [call.args[0] for call in mock_clean.call_args_list], server_vms
    )

  @flagsaver.flagsaver(
      cassandra_yaml_template_path='cassandra/cassandra-lssd.yaml.j2'
  )
  def test_configure_renders_default_lssd_yaml_without_jvm_override(self):
    server_vm = self._CreateTestVm('server-0', '10.0.0.1')
    rendered_files = {}

    def capture_render(template_path, remote_path, context):
      rendered_files[remote_path] = _render_jinja_file(template_path, context)

    server_vm.RenderTemplate.side_effect = capture_render

    cassandra.Configure(
        server_vm,
        [server_vm],
        custom_cassandra_conf={'concurrent_reads': 32, 'concurrent_writes': 64},
    )

    parsed_yaml = yaml.safe_load(rendered_files['~/cassandra.yaml'])
    self.assertEqual(parsed_yaml['row_cache_size'], '0MiB')
    self.assertEqual(parsed_yaml['key_cache_size'], '0MiB')
    self.assertNotIn('disk_access_mode', parsed_yaml)
    self.assertEqual(parsed_yaml['concurrent_reads'], 32)
    self.assertEqual(parsed_yaml['concurrent_writes'], 64)
    server_vm.RemoteCommand.assert_any_call(
        'sudo cp ~/cassandra.yaml'
        f' {cassandra.CASSANDRA_DIR}/conf/cassandra.yaml'
    )
    for call in server_vm.RemoteCommand.call_args_list:
      self.assertNotIn('jvm-server.options', call.args[0])

  def test_default_flags_do_not_impact_existing_pd_benchmark(self):
    server_vm = self._CreateTestVm('server-0', '10.0.0.1')
    rendered_files = {}

    def capture_render(template_path, remote_path, context):
      rendered_files[remote_path] = _render_jinja_file(template_path, context)

    server_vm.RenderTemplate.side_effect = capture_render

    cassandra.Configure(
        server_vm,
        [server_vm],
        custom_cassandra_conf={'concurrent_reads': 32, 'concurrent_writes': 64},
    )
    cassandra.CreateKeyspace(
        server_vm,
        replication_factor=3,
        is_row_cache_enabled=True,
        compaction_strategy=FLAGS.cassandra_compaction_strategy,
    )

    parsed_yaml = yaml.safe_load(rendered_files['~/cassandra.yaml'])
    self.assertEqual(parsed_yaml['row_cache_size'], '0MiB')
    self.assertIsNone(parsed_yaml['key_cache_size'])
    self.assertNotIn('disk_access_mode', parsed_yaml)
    for call in server_vm.RemoteCommand.call_args_list:
      self.assertNotIn('jvm-server.options', call.args[0])
    self.assertIn(
        "WITH compaction = {'class': 'SizeTieredCompactionStrategy'};",
        rendered_files['~/enable-row-caching.cql'],
    )
    spec, server_vms, client_vm = self._CreateBenchmarkSpec()
    metadata = cassandra_stress_benchmark.GenerateMetadataFromFlags(
        spec, server_vms, [client_vm]
    )
    self.assertNotIn('row_cache_size', metadata)
    self.assertNotIn('key_cache_size', metadata)
    self.assertNotIn('disk_access_mode', metadata)
    self.assertNotIn('jvm_heap_size', metadata)

  @flagsaver.flagsaver(
      cassandra_yaml_template_path='cassandra/cassandra-lssd.yaml.j2',
      cassandra_row_cache_size='4096MiB',
      cassandra_key_cache_size='256MiB',
      cassandra_disk_access_mode='standard',
      cassandra_jvm_heap_size='16G',
  )
  def test_configure_renders_tuning_flags_and_jvm_heap(self):
    server_vm = self._CreateTestVm('server-0', '10.0.0.1')
    rendered_files = {}

    def capture_render(template_path, remote_path, context):
      rendered_files[remote_path] = _render_jinja_file(template_path, context)

    server_vm.RenderTemplate.side_effect = capture_render

    cassandra.Configure(
        server_vm,
        [server_vm],
        custom_cassandra_conf={'concurrent_reads': 32, 'concurrent_writes': 64},
    )

    parsed_yaml = yaml.safe_load(rendered_files['~/cassandra.yaml'])
    self.assertEqual(parsed_yaml['row_cache_size'], '4096MiB')
    self.assertEqual(parsed_yaml['key_cache_size'], '256MiB')
    self.assertEqual(parsed_yaml['disk_access_mode'], 'standard')
    jvm_options_path = f'{cassandra.CASSANDRA_DIR}/conf/jvm-server.options'
    server_vm.RemoteCommand.assert_any_call(
        f"sudo sed -i '/^-Xm[sx]/d' {jvm_options_path}"
    )
    server_vm.RemoteCommand.assert_any_call(
        f"printf '\\n-Xms16G\\n-Xmx16G\\n' |"
        f' sudo tee -a {jvm_options_path} >/dev/null'
    )

  def test_create_keyspace_with_row_cache_applies_compaction_strategy(self):
    server_vm = self._CreateTestVm('server-0', '10.0.0.1')
    rendered_files = {}

    def capture_render(template_path, remote_path, context):
      rendered_files[remote_path] = _render_jinja_file(template_path, context)

    server_vm.RenderTemplate.side_effect = capture_render

    cassandra.CreateKeyspace(
        server_vm,
        replication_factor=3,
        is_row_cache_enabled=True,
        compaction_strategy='UnifiedCompactionStrategy',
    )

    self.assertIn(
        "WITH compaction = {'class': 'UnifiedCompactionStrategy'};",
        rendered_files['~/enable-row-caching.cql'],
    )

  @flagsaver.flagsaver(
      cassandra_compaction_strategy='UnifiedCompactionStrategy',
      is_row_cache_enabled=True,
      cassandra_row_cache_size='4096MiB',
      cassandra_key_cache_size='256MiB',
      cassandra_disk_access_mode='mmap',
      cassandra_jvm_heap_size='32G',
  )
  def test_generate_metadata_records_tuning_flags(self):
    spec, server_vms, client_vm = self._CreateBenchmarkSpec()

    metadata = cassandra_stress_benchmark.GenerateMetadataFromFlags(
        spec, server_vms, [client_vm]
    )

    self.assertEqual(
        metadata['compaction_strategy'], 'UnifiedCompactionStrategy'
    )
    self.assertTrue(metadata['is_row_cache_enabled'])
    self.assertEqual(metadata['row_cache_size'], '4096MiB')
    self.assertEqual(metadata['key_cache_size'], '256MiB')
    self.assertEqual(metadata['disk_access_mode'], 'mmap')
    self.assertEqual(metadata['jvm_heap_size'], '32G')

  @flagsaver.flagsaver(
      cassandra_yaml_template_path='cassandra/cassandra-lssd.yaml.j2',
      cassandra_key_cache_size='512MiB',
      cassandra_disk_access_mode='mmap_index_only',
      cassandra_jvm_heap_size=None,
  )
  def test_reconfigure_updates_yaml_and_clears_jvm_heap_without_wiping_data(
      self,
  ):
    server_vm = self._CreateTestVm('server-0', '10.0.0.1')
    rendered_files = {}

    def capture_render(template_path, remote_path, context):
      rendered_files[remote_path] = _render_jinja_file(template_path, context)

    server_vm.RenderTemplate.side_effect = capture_render

    cassandra.Reconfigure(
        server_vm,
        [server_vm],
        custom_cassandra_conf={
            'concurrent_reads': 128,
            'concurrent_writes': 256,
        },
    )

    parsed_yaml = yaml.safe_load(rendered_files['~/cassandra.yaml'])
    self.assertEqual(parsed_yaml['key_cache_size'], '512MiB')
    self.assertEqual(parsed_yaml['disk_access_mode'], 'mmap_index_only')
    self.assertEqual(parsed_yaml['concurrent_reads'], 128)
    self.assertEqual(parsed_yaml['concurrent_writes'], 256)
    jvm_options_path = f'{cassandra.CASSANDRA_DIR}/conf/jvm-server.options'
    commands = [call.args[0] for call in server_vm.RemoteCommand.call_args_list]
    self.assertIn(f"sudo sed -i '/^-Xm[sx]/d' {jvm_options_path}", commands)
    for cmd in commands:
      self.assertNotIn('-Xms', cmd)
      self.assertNotIn('mkdir ', cmd)
      self.assertNotIn('apt ', cmd)

  def test_start_passes_pidfile_flag(self):
    server_vm = self._CreateTestVm('server-0', '10.0.0.1')

    cassandra.Start(server_vm)

    server_vm.RemoteCommand.assert_called_once_with(
        f'{cassandra.GetCassandraPath()} -p {cassandra.CASSANDRA_PID}'
    )

  def test_stop_and_wait_drains_and_waits_for_daemon_exit(self):
    server_vm = self._CreateTestVm('server-0', '10.0.0.1')
    server_vm.RemoteCommandWithReturnCode.side_effect = [
        ('1234\n', '', 0),
        ('', '', 1),
    ]
    mock_sleep = self.enter_context(
        mock.patch.object(time, 'sleep', autospec=True, spec_set=True)
    )

    cassandra.StopAndWait(server_vm)

    self.assertLen(server_vm.RemoteCommand.call_args_list, 3)
    self.assertEqual(
        server_vm.RemoteCommand.call_args_list[0].args[0],
        f'{cassandra.GetNodetoolPath()} drain',
    )
    self.assertIn(
        cassandra.DAEMON_PATTERN,
        server_vm.RemoteCommand.call_args_list[1].args[0],
    )
    self.assertEqual(
        server_vm.RemoteCommand.call_args_list[2].args[0],
        f'rm -f {cassandra.CASSANDRA_PID}',
    )
    self.assertEqual(server_vm.RemoteCommandWithReturnCode.call_count, 2)
    mock_sleep.assert_called_once_with(1)

  def test_stop_and_wait_escalates_to_sigkill_after_graceful_timeout(self):
    server_vm = self._CreateTestVm('server-0', '10.0.0.1')
    server_vm.RemoteCommandWithReturnCode.side_effect = [
        ('1234\n', '', 0)
    ] * (cassandra.STOP_TIMEOUT_SECONDS + 1) + [('', '', 1)]
    self.enter_context(
        mock.patch.object(time, 'sleep', autospec=True, spec_set=True)
    )

    cassandra.StopAndWait(server_vm)

    commands = [call.args[0] for call in server_vm.RemoteCommand.call_args_list]
    self.assertIn(
        f"sudo pkill -9 -f '{cassandra.DAEMON_PATTERN}'",
        commands,
    )
    self.assertEqual(commands[-1], f'rm -f {cassandra.CASSANDRA_PID}')

  def test_stop_and_wait_raises_when_daemon_survives_sigkill(self):
    server_vm = self._CreateTestVm('server-0', '10.0.0.1')
    server_vm.RemoteCommandWithReturnCode.return_value = ('1234\n', '', 0)
    self.enter_context(
        mock.patch.object(time, 'sleep', autospec=True, spec_set=True)
    )

    with self.assertRaises(errors.VirtualMachine.VmStateError):
      cassandra.StopAndWait(server_vm)

    commands = [call.args[0] for call in server_vm.RemoteCommand.call_args_list]
    self.assertNotIn(f'rm -f {cassandra.CASSANDRA_PID}', commands)

  @flagsaver.flagsaver(
      cassandra_reconfigure_on_run=True,
      cassandra_concurrent_reads=128,
      cassandra_concurrent_writes=256,
      cassandra_concurrent_compactors=8,
      cassandra_compaction_throughput_mb=0,
      cassandra_stress_threads=['200'],
      cassandra_stress_run_duration=None,
  )
  def test_run_with_reconfigure_on_run_restarts_cluster_and_applies_settings(
      self,
  ):
    spec, server_vms, _ = self._CreateBenchmarkSpec()
    for server_vm in server_vms:
      server_vm.num_cpus = 16
      server_vm.RemoteCommand.return_value = (
          '20:11:37 up 1 day, 1 user, load average: 1.00, 2.00, 1.50',
          '',
      )
    mock_stop_and_wait = self.enter_context(
        mock.patch.object(
            cassandra, 'StopAndWait', autospec=True, spec_set=True
        )
    )
    mock_reconfigure = self.enter_context(
        mock.patch.object(
            cassandra, 'Reconfigure', autospec=True, spec_set=True
        )
    )
    mock_start_cluster = self.enter_context(
        mock.patch.object(
            cassandra, 'StartCluster', autospec=True, spec_set=True
        )
    )
    mock_update_compactors = self.enter_context(
        mock.patch.object(
            cassandra,
            'UpdateCassandraConcurrentCompactorCount',
            autospec=True,
            spec_set=True,
        )
    )
    mock_update_throughput = self.enter_context(
        mock.patch.object(
            cassandra,
            'UpdateCassandraCompactionThroughput',
            autospec=True,
            spec_set=True,
        )
    )
    self.enter_context(
        mock.patch.object(
            cassandra,
            'GetPendingTaskCountFromCompactionStats',
            autospec=True,
            spec_set=True,
            return_value=[0, 0],
        )
    )
    self.enter_context(
        mock.patch.object(
            cassandra,
            'GetCassandraVersion',
            autospec=True,
            spec_set=True,
            return_value='5.0.0',
        )
    )
    self.enter_context(
        mock.patch.object(
            cassandra, 'GetNodetoolStatus', autospec=True, spec_set=True
        )
    )
    self.enter_context(
        mock.patch.object(
            cassandra_stress_benchmark,
            'RunCassandraStressOnClient',
            autospec=True,
            spec_set=True,
        )
    )
    self.enter_context(
        mock.patch.object(
            cassandra_stress_benchmark,
            'CollectResultFile',
            autospec=True,
            spec_set=True,
            side_effect=lambda vm, results: results.update({
                metric: [100.0]
                for metric in cassandra_stress_benchmark.RESULTS_METRICS
            }),
        )
    )

    samples = cassandra_stress_lssd_benchmark.Run(spec)

    self.assertEqual(mock_stop_and_wait.call_count, 2)
    self.assertEqual(mock_reconfigure.call_count, 2)
    mock_reconfigure.assert_any_call(
        server_vms[0],
        [server_vms[0]],
        {'concurrent_reads': 128, 'concurrent_writes': 256},
    )
    mock_start_cluster.assert_called_once_with(server_vms[0], [server_vms[1]])
    mock_update_compactors.assert_any_call(server_vms[0], 8)
    mock_update_throughput.assert_any_call(server_vms[0], 0)
    max_op_sample = next(s for s in samples if s.metric == 'max_op_rate')
    self.assertTrue(max_op_sample.metadata['reconfigure_on_run'])
    self.assertEqual(
        max_op_sample.metadata['num_cassandra_stress_threads'], 200
    )

  @flagsaver.flagsaver(
      cassandra_reconfigure_on_run=False,
      cassandra_stress_threads=['100', '200', '1000'],
      cassandra_stress_run_duration=None,
  )
  def test_run_without_reconfigure_skips_restart_and_sorts_threads_numerically(
      self,
  ):
    spec, server_vms, _ = self._CreateBenchmarkSpec()
    for server_vm in server_vms:
      server_vm.num_cpus = 16
      server_vm.RemoteCommand.return_value = (
          '20:11:37 up 1 day, 1 user, load average: 1.00, 2.00, 1.50',
          '',
      )
    mock_stop_and_wait = self.enter_context(
        mock.patch.object(
            cassandra, 'StopAndWait', autospec=True, spec_set=True
        )
    )
    mock_reconfigure = self.enter_context(
        mock.patch.object(
            cassandra, 'Reconfigure', autospec=True, spec_set=True
        )
    )
    mock_start_cluster = self.enter_context(
        mock.patch.object(
            cassandra, 'StartCluster', autospec=True, spec_set=True
        )
    )
    self.enter_context(
        mock.patch.object(
            cassandra,
            'GetPendingTaskCountFromCompactionStats',
            autospec=True,
            spec_set=True,
            return_value=[0, 0],
        )
    )
    self.enter_context(
        mock.patch.object(
            cassandra,
            'GetCassandraVersion',
            autospec=True,
            spec_set=True,
            return_value='5.0.0',
        )
    )
    self.enter_context(
        mock.patch.object(
            cassandra, 'GetNodetoolStatus', autospec=True, spec_set=True
        )
    )
    recorded_threads = []
    self.enter_context(
        mock.patch.object(
            cassandra_stress_benchmark,
            'RunCassandraStressOnClient',
            autospec=True,
            spec_set=True,
            side_effect=lambda *args: recorded_threads.append(
                args[5]['num_cassandra_stress_threads']
            ),
        )
    )
    self.enter_context(
        mock.patch.object(
            cassandra_stress_benchmark,
            'CollectResultFile',
            autospec=True,
            spec_set=True,
            side_effect=lambda vm, results: results.update({
                metric: [100.0]
                for metric in cassandra_stress_benchmark.RESULTS_METRICS
            }),
        )
    )

    cassandra_stress_lssd_benchmark.Run(spec)

    mock_stop_and_wait.assert_not_called()
    mock_reconfigure.assert_not_called()
    mock_start_cluster.assert_not_called()
    # 100 -> 200 plateaus (op rate 100.0), so midpoint (100+200)//2=150 runs
    # rather than crashing on str // int or sorting '1000' before '200'.
    self.assertEqual(recorded_threads, [100, 200, 150])


if __name__ == '__main__':
  unittest.main()
