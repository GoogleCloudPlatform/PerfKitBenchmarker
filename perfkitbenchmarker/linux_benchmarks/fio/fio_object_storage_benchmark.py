# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Run FIO Benchmark on object storage via FUSE."""
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks.fio import constants
from perfkitbenchmarker.linux_benchmarks.fio import flags as fio_flags
from perfkitbenchmarker.linux_benchmarks.fio import utils


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'fio_object_storage'
BENCHMARK_CONFIG = """
fio_object_storage:
  description: Runs fio.
  vm_groups:
    default:
      vm_spec: *default_dual_core
      disk_spec: *default_500_gb
      vm_count: 2
  flags:
    boot_disk_size: 300
    data_disk_type: object_storage
    scratch_dir: fuse_dir
    fio_target_mode: against_file_with_fill
    fio_generate_scenarios: rand_8k_read_100GB_iodepth-1_numjobs-1
    fio_fill_size: 100G
    fio_ioengine: sync
    fio_ramptime: 0
    fio_runtime: 30
    gcloud_scopes: https://www.googleapis.com/auth/devstorage.full_control
"""


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  disk_spec = config['vm_groups']['default']['disk_spec']
  for cloud in disk_spec:
    disk_spec[cloud]['mount_point'] = None
  return config


def Prepare(spec: benchmark_spec.BenchmarkSpec):
  writer_vm = spec.vm_groups['default'][1]
  writer_vm.Install('fio')
  utils.PrefillIfEnabled(writer_vm, constants.FIO_PATH, use_directory=True)
  test_vm = spec.vm_groups['default'][0]
  test_vm.Install('fio')


def Run(spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs FIO benchmark on raw devices."""
  vm = spec.vm_groups['default'][0]
  benchmark_params = {}
  job_file_str = utils.GenerateJobFile(
      vm.scratch_disks,
      fio_flags.FIO_GENERATE_SCENARIOS.value,
      benchmark_params,
      job_file='fio-object-storage.job',
  )
  return utils.RunTest(
      vm, constants.FIO_PATH, job_file_str, latency_measure='lat'
  )


def Cleanup(_):
  pass
