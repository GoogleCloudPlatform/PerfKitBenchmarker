# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs SPEC CPU2017.

From the SPEC CPU2017 documentation:
The SPEC CPU 2017 benchmark package contains SPEC's next-generation,
industry-standardized, CPU intensive suites for measuring and comparing
compute intensive performance, stressing a system's processor,
memory subsystem and compiler.

SPEC CPU2017 homepage: http://www.spec.org/cpu2017/
"""

from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import speccpu


INT_SUITE = ['perlbench', 'gcc', 'mcf', 'omnetpp',
             'xalancbmk', 'x264', 'deepsjeng', 'leela',
             'exchange2', 'xz']
INTSPEED_SUITE = [benchmark + '_s' for benchmark in INT_SUITE]
INTRATE_SUITE = [benchmark + '_r' for benchmark in INT_SUITE]

COMMON_FP_SUITE = ['bwaves', 'cactuBSSN', 'lbm', 'wrf', 'cam4', 'imagick',
                   'nab', 'fotonik3d', 'roms']
FPSPEED_SUITE = [benchmark + '_s' for benchmark in COMMON_FP_SUITE] + ['pop2_s']
FPRATE_SUITE = [benchmark + '_r' for benchmark in COMMON_FP_SUITE] + [
    'namd_r', 'parest_r', 'povray_r', 'blender_r']

FLAGS = flags.FLAGS
flags.DEFINE_list('spec17_subset', ['intspeed', 'fpspeed', 'intrate', 'fprate'],
                  'Specify which speccpu2017 tests to run. Accepts a list of '
                  'benchmark suites (intspeed, fpspeed, intrate, fprate) '
                  'or individual benchmark names. Defaults to all suites.')
flags.DEFINE_integer('spec17_copies', None,
                     'Number of copies to run for rate tests. If not set '
                     'default to number of cpu cores using lscpu.')
flags.DEFINE_integer('spec17_threads', None,
                     'Number of threads to run for speed tests. If not set '
                     'default to number of cpu threads using lscpu.')
flags.DEFINE_boolean('spec17_fdo', False,
                     'Run with feedback directed optimization on peak. '
                     'Default to False.')


BENCHMARK_NAME = 'speccpu2017'
BENCHMARK_CONFIG = """
speccpu2017:
  description: Runs SPEC CPU2017
  vm_groups:
    default:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
      os_type: ubuntu1604
  speccpu:
    runspec_config: pkb-crosstool-llvm-linux-x86-fdo.cfg
"""

_SPECCPU2017_DIR = 'cpu2017'
_SPECCPU2017_TAR = 'speccpu2017.tgz'
_TAR_REQUIRED_MEMBERS = 'cpu2017', 'cpu2017/bin/runcpu'
_LOG_FORMAT = r'Est. (SPEC.*2017_.*_base)\s*(\S*)'
BENCHMARK_DATA = {_SPECCPU2017_TAR: None}
KB_TO_GB_MULTIPLIER = 1000000

LOG_FILENAME = {
    'fprate': 'CPU2017.001.fprate.refrate.txt',
    'fpspeed': 'CPU2017.001.fpspeed.refspeed.txt',
    'intrate': 'CPU2017.001.intrate.refrate.txt',
    'intspeed': 'CPU2017.001.intspeed.refspeed.txt',
}


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """FDO is only allow with peak measurement.

  Args:
    benchmark_config: benchmark_config
  Raises:
    errors.Config.InvalidValue: On invalid flag setting.
  """
  del benchmark_config  # unused
  if FLAGS.spec17_fdo and FLAGS.spec_runmode == 'base':
    raise errors.Config.InvalidValue(
        'Feedback Directed Optimization is not allowed with base report.')


def CheckVmPrerequisites(vm):
  """Checks the system memory on this vm.

  Rate runs require 2 GB minimum system memory.
  Speed runs require 16 GB minimum system memory.

  Args:
    vm: virtual machine to run spec on.
  Raises:
    errors.Config.InvalidValue: On insufficient vm memory.
  """
  available_memory = vm.total_free_memory_kb
  if 'intspeed' in FLAGS.spec17_subset or 'fpspeed' in FLAGS.spec17_subset:
    if available_memory < 16 * KB_TO_GB_MULTIPLIER:
      raise errors.Config.InvalidValue(
          'Available memory of %s GB is insufficient for spec17 speed runs.'
          % (available_memory / KB_TO_GB_MULTIPLIER))
  if 'intrate' in FLAGS.spec17_subset or 'fprate' in FLAGS.spec17_subset:
    if available_memory < 2 * KB_TO_GB_MULTIPLIER:
      raise errors.Config.InvalidValue(
          'Available memory of %s GB is insufficient for spec17 rate runs.'
          % (available_memory / KB_TO_GB_MULTIPLIER))


def Prepare(benchmark_spec):
  """Installs SPEC CPU2017 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  CheckVmPrerequisites(vm)
  install_config = speccpu.SpecInstallConfigurations()
  install_config.benchmark_name = BENCHMARK_NAME
  install_config.base_spec_dir = _SPECCPU2017_DIR
  install_config.base_tar_file_path = _SPECCPU2017_TAR
  install_config.required_members = _TAR_REQUIRED_MEMBERS
  install_config.log_format = _LOG_FORMAT
  install_config.runspec_config = benchmark_spec.config.speccpu.runspec_config
  speccpu.InstallSPECCPU(vm, install_config)
  vm.Install('speccpu2017_dependencies')


def Run(benchmark_spec):
  """Runs SPEC CPU2017 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vm = benchmark_spec.vms[0]

  # swap only if necessary; free local node memory and avoid remote memory;
  # reset caches; set stack size to unlimited
  # Also consider setting enable_transparent_hugepages flag to true
  cmd = ('echo 1 | sudo tee /proc/sys/vm/swappiness && '
         'echo 1 | sudo tee /proc/sys/vm/zone_reclaim_mode && '
         'sync ; echo 3 | sudo tee /proc/sys/vm/drop_caches && '
         'ulimit -s unlimited && ')

  cmd += 'runcpu '

  version_specific_parameters = []
  # rate runs require 2 GB minimum system main memory per copy,
  # not including os overhead
  # Refer to: https://www.spec.org/cpu2017/Docs/system-requirements.html#memory
  copies = min(vm.num_cpus,
               vm.total_free_memory_kb / (2 * KB_TO_GB_MULTIPLIER))
  version_specific_parameters.append(' --copies=%s ' %
                                     (FLAGS.spec17_copies or copies))
  version_specific_parameters.append(' --threads=%s ' %
                                     (FLAGS.spec17_threads or vm.num_cpus))

  if FLAGS.spec17_fdo:
    version_specific_parameters.append('--feedback ')
    vm.RemoteCommand('cd /scratch/cpu2017; mkdir fdo_profiles')

  speccpu.Run(vm, cmd, ' '.join(FLAGS.spec17_subset),
              version_specific_parameters)

  partial_results = True
  # Do not allow partial results if any benchmark subset is a full suite.
  for benchmark_subset in FLAGS.benchmark_subset:
    if benchmark_subset in ['intspeed', 'fpspeed', 'intrate', 'fprate']:
      partial_results = False

  log_files = set()
  for test in FLAGS.spec17_subset:
    if test in LOG_FILENAME:
      log_files.add(LOG_FILENAME[test])
    else:
      if test in INTSPEED_SUITE:
        log_files.add(LOG_FILENAME['intspeed'])
      elif test in INTRATE_SUITE:
        log_files.add(LOG_FILENAME['intrate'])
      elif test in FPSPEED_SUITE:
        log_files.add(LOG_FILENAME['fpspeed'])
      elif test in FPRATE_SUITE:
        log_files.add(LOG_FILENAME['fprate'])

  return speccpu.ParseOutput(vm, log_files, partial_results, None)


def Cleanup(benchmark_spec):
  """Cleans up SPEC CPU2017 from the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  speccpu.Uninstall(vm)
