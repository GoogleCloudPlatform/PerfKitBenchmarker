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
from perfkitbenchmarker.linux_packages import speccpu2006


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


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Installs SPEC CPU2006 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  vm.Install('speccpu2006')
  # Set attribute outside of the install function, so benchmark will work
  # even with --install_packages=False.
  config = speccpu2006.GetSpecInstallConfig(vm.GetScratchDir())
  setattr(vm, speccpu.VM_STATE_ATTR, config)


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
    version_specific_parameters.append(' --rate=%s ' % vm.NumCpusForBenchmark())
  else:
    version_specific_parameters.append(' --speed ')
  speccpu.Run(vm, 'runspec',
              FLAGS.benchmark_subset, version_specific_parameters)

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
