# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs SPEC CPU2006.

From the SPEC CPU2006 documentation:
"The SPEC CPU 2006 benchmark is SPEC's next-generation, industry-standardized,
CPU-intensive benchmark suite, stressing a system's processor, memory subsystem
and compiler."

SPEC CPU2006 homepage: http://www.spec.org/cpu2006/
"""

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import speccpu


FLAGS = flags.FLAGS

_SPECINT_BENCHMARKS = frozenset([
    'perlbench', 'bzip2', 'gcc', 'mcf', 'gobmk', 'hmmer', 'sjeng',
    'libquantum', 'h264ref', 'omnetpp', 'astar', 'xalancbmk'])
_SPECFP_BENCHMARKS = frozenset([
    'bwaves', 'gamess', 'milc', 'zeusmp', 'gromacs', 'cactusADM',
    'leslie3d', 'namd', 'dealII', 'soplex', 'povray', 'calculix',
    'GemsFDTD', 'tonto', 'lbm', 'wrf', 'sphinx3'])
_SPECCPU_SUBSETS = frozenset(['int', 'fp', 'all'])

flags.DEFINE_enum(
    'benchmark_subset', 'int',
    _SPECFP_BENCHMARKS | _SPECINT_BENCHMARKS | _SPECCPU_SUBSETS,
    'Used by the PKB speccpu2006 benchmark. Specifies a subset of SPEC CPU2006 '
    'benchmarks to run.')
flags.DEFINE_enum('runspec_metric', 'rate', ['rate', 'speed'],
                  'SPEC test to run. Speed is time-based metric, rate is '
                  'throughput-based metric.')

BENCHMARK_NAME = 'speccpu2006'
BENCHMARK_CONFIG = """
speccpu2006:
  description: Runs SPEC CPU2006
  vm_groups:
    default:
      vm_spec: *default_single_core
      disk_spec: *default_50_gb
"""

_MOUNT_DIR = 'cpu2006_mnt'
_SPECCPU2006_DIR = 'cpu2006'
_SPECCPU2006_ISO = 'cpu2006-1.2.iso'
_SPECCPU2006_TAR = 'cpu2006v1.2.tgz'
_TAR_REQUIRED_MEMBERS = 'cpu2006', 'cpu2006/bin/runspec'
_LOG_FORMAT = r'Est. (SPEC.*_base2006)\s*(\S*)'
# This benchmark can be run with an .iso file in the data directory, a tar file
# in the data directory, or a tar file preprovisioned in cloud storage. To run
# this benchmark with tar file preprovisioned in cloud storage, update the
# following dict with md5sum of the file in cloud storage.
BENCHMARK_DATA = {_SPECCPU2006_TAR: None}


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Installs SPEC CPU2006 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  install_config = speccpu.SpecInstallConfigurations()
  install_config.benchmark_name = BENCHMARK_NAME
  install_config.base_mount_dir = _MOUNT_DIR
  install_config.base_spec_dir = _SPECCPU2006_DIR
  install_config.base_iso_file_path = _SPECCPU2006_ISO
  install_config.base_tar_file_path = _SPECCPU2006_TAR
  install_config.required_members = _TAR_REQUIRED_MEMBERS
  install_config.log_format = _LOG_FORMAT
  speccpu.Install(vm, install_config)


def Run(benchmark_spec):
  """Runs SPEC CPU2006 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vm = benchmark_spec.vms[0]

  version_specific_parameters = []
  if FLAGS.runspec_metric == 'rate':
    version_specific_parameters.append(' --rate=%s ' % vm.num_cpus)
  else:
    version_specific_parameters.append(' --speed ')
  speccpu.Run(vm, FLAGS.benchmark_subset, version_specific_parameters)

  log_files = []
  # FIXME(liquncheng): Only reference runs generate SPEC scores. The log
  # id is hardcoded as 001, which might change with different runspec
  # parameters. SPEC CPU2006 will generate different logs for build, test
  # run, training run and ref run.
  if FLAGS.benchmark_subset in _SPECINT_BENCHMARKS | set(['int', 'all']):
    log_files.append('CINT2006.001.ref.txt')
  if FLAGS.benchmark_subset in _SPECFP_BENCHMARKS | set(['fp', 'all']):
    log_files.append('CFP2006.001.ref.txt')
  partial_results = FLAGS.benchmark_subset not in _SPECCPU_SUBSETS

  return speccpu.ParseOutput(vm, log_files, partial_results,
                             FLAGS.runspec_metric)


def Cleanup(benchmark_spec):
  """Cleans up SPEC CPU2006 from the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  speccpu.Uninstall(vm)
