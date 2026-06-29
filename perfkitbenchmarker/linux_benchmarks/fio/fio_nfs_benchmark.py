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
"""Run FIO Benchmark on raw devices."""
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks.fio import constants
from perfkitbenchmarker.linux_benchmarks.fio import flags as fio_flags
from perfkitbenchmarker.linux_benchmarks.fio import utils


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'fio_nfs'
BENCHMARK_CONFIG = """
fio_nfs:
  description: Runs fio on NFS.
  vm_groups:
    default:
      vm_spec: *default_dual_core
      disk_spec: *default_500_gb
      vm_count: 1
"""
JOB_FILE = 'fio-parent.job'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Perform flag checks."""
  del benchmark_config  # unused
  if not utils.AgainstNFS():
    errors.Setup.InvalidFlagConfigurationError(
        'fio_nfs_benchmark only supports against NFS target mode.'
    )
  if fio_flags.FIO_FILL_SIZE.value[-1].upper() not in ('K', 'M', 'G'):
    errors.Setup.InvalidFlagConfigurationError(
        'fio_nfs_benchmark requires --fio_fill_size to be set.'
    )


def Prepare(spec: benchmark_spec.BenchmarkSpec):
  vm = spec.vms[0]
  if len(vm.scratch_disks) != 1:
    raise errors.Setup.InvalidFlagConfigurationError(
        'FIO NFS benchmark can\'t have more than one scratch disk.'
    )
  vm.Install('fio')
  utils.WriteFile(
      vm,
      disk=vm.scratch_disks[0],
      fill_size=fio_flags.FIO_FILL_SIZE.value,
      exec_path=constants.FIO_PATH,
  )


def Run(spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs FIO benchmark on raw devices."""
  vm = spec.vms[0]
  benchmark_params = {}
  job_file_str = utils.GenerateJobFile(
      vm.scratch_disks,
      fio_flags.FIO_GENERATE_SCENARIOS.value,
      benchmark_params,
  )
  return utils.RunTest(vm, constants.FIO_PATH, job_file_str)


def Cleanup(_):
  pass
