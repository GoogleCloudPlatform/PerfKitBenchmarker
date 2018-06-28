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
from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import INSTALL_DIR
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


BENCHMARK_NAME = 'speccpu2017'
BENCHMARK_CONFIG = """
speccpu2017:
  description: Runs SPEC CPU2017
  vm_groups:
    default:
      vm_spec: *default_single_core
      disk_spec: *default_50_gb
"""

_SPECCPU2017_DIR = 'cpu2017'
_SPECCPU2017_TAR = 'speccpu2017.tgz'
_TAR_REQUIRED_MEMBERS = 'cpu2017', 'cpu2017/bin/runcpu'
_LOG_FORMAT = r'Est. (SPEC.*2017_(fp|int)_base)\s*(\S*)'
BENCHMARK_DATA = {_SPECCPU2017_TAR: None}
LLVM_TAR = 'clang+llvm-3.9.0-x86_64-linux-gnu-ubuntu-16.04.tar.xz'
LLVM_TAR_URL = 'http://releases.llvm.org/3.9.0/{0}'.format(LLVM_TAR)
OPENMP_TAR = 'libomp_20160808_oss.tgz'
OPENMP_TAR_URL = 'https://www.openmprtl.org/sites/default/files/{0}'.format(
    OPENMP_TAR)

LOG_FILENAME = {
    'fprate': 'CPU2017.001.fprate.refrate.txt',
    'fpspeed': 'CPU2017.001.fpspeed.refspeed.txt',
    'intrate': 'CPU2017.001.intrate.refrate.txt',
    'intspeed': 'CPU2017.001.intspeed.refspeed.txt',
}


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
  install_config.base_spec_dir = _SPECCPU2017_DIR
  install_config.base_tar_file_path = _SPECCPU2017_TAR
  install_config.required_members = _TAR_REQUIRED_MEMBERS
  install_config.log_format = _LOG_FORMAT
  speccpu.Install(vm, install_config)

  vm.RemoteCommand('cd {0} && wget {1} && tar xf {2}'.format(
      INSTALL_DIR, LLVM_TAR_URL, LLVM_TAR))
  vm.RemoteCommand('cd {0} && wget {1} && tar xf {2}'.format(
      INSTALL_DIR, OPENMP_TAR_URL, OPENMP_TAR))


def Run(benchmark_spec):
  """Runs SPEC CPU2017 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vm = benchmark_spec.vms[0]

  speccpu.Run(vm, 'runcpu', ' '.join(FLAGS.spec17_subset))

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

  return speccpu.ParseOutput(vm, log_files, False, None)


def Cleanup(benchmark_spec):
  """Cleans up SPEC CPU2006 from the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  speccpu.Uninstall(vm)
