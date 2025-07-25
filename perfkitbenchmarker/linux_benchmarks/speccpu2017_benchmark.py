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

import os
import re
import time

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import build_tools
from perfkitbenchmarker.linux_packages import speccpu
from perfkitbenchmarker.linux_packages import speccpu2017

INT_SUITE = [
    'perlbench',
    'gcc',
    'mcf',
    'omnetpp',
    'xalancbmk',
    'x264',
    'deepsjeng',
    'leela',
    'exchange2',
    'xz',
]
INTSPEED_SUITE = [benchmark + '_s' for benchmark in INT_SUITE]
INTRATE_SUITE = [benchmark + '_r' for benchmark in INT_SUITE]

COMMON_FP_SUITE = [
    'bwaves',
    'cactuBSSN',
    'lbm',
    'wrf',
    'cam4',
    'imagick',
    'nab',
    'fotonik3d',
    'roms',
]
FPSPEED_SUITE = [benchmark + '_s' for benchmark in COMMON_FP_SUITE] + ['pop2_s']
FPRATE_SUITE = [benchmark + '_r' for benchmark in COMMON_FP_SUITE] + [
    'namd_r',
    'parest_r',
    'povray_r',
    'blender_r',
]

FLAGS = flags.FLAGS

flags.DEFINE_boolean(
    'spec17_build_only',
    False,
    "Compile benchmarks only, but don't run benchmarks. "
    'Defaults to False. The benchmark fails if the build '
    'fails.',
)
flags.DEFINE_boolean(
    'spec17_rebuild',
    True,
    'Rebuild spec binaries, defaults to True. Set to False '
    'when using run_stage_iterations > 1 to avoid recompiling',
)
SPEC17_GCC_FLAGS= flags.DEFINE_string(
    'spec17_gcc_flags',
    None,
    'Flags to be used to override the default OPTIMIZE used to compile SPEC for'
    ' GCC.',
)
flags.DEFINE_boolean(
    'spec17_best_effort',
    False,
    'Best effort run of spec. Allow missing results without failing.',
)
flags.DEFINE_string(
    'spec17_numa_bind_config',
    None,
    'Name of the config file to use for specifying NUMA binding. '
    'None by default. To enable numa binding, ensure the runspec_config file, '
    'contains "include: numactl.inc". In addition, setting to "auto", will '
    'attempt to pin to local numa node and pin each SIR copy to a '
    'exclusive hyperthread.',
)

BENCHMARK_NAME = 'speccpu2017'
BENCHMARK_CONFIG = """
speccpu2017:
  description: Runs SPEC CPU2017
  vm_groups:
    default:
      vm_spec: *default_dual_core
      disk_spec: *default_500_gb
  flags:
    build_fortran: True
    gcc_version: "11"
    enable_transparent_hugepages: True
    runspec_tar: cpu2017-1.1.8.tar.gz
"""

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
        'Feedback Directed Optimization is not allowed with base report.'
    )


def CheckVmPrerequisites(vm):
  """Checks the system memory on this vm.

  Rate runs require 2 GB minimum system memory.
  Speed runs require 16 GB minimum system memory.
  Taken from https://www.spec.org/cpu2017/Docs/system-requirements.html

  Args:
    vm: virtual machine to run spec on.

  Raises:
    errors.Config.InvalidValue: On insufficient vm memory.
  """
  available_memory = vm.total_free_memory_kb
  if 'intspeed' in FLAGS.spec17_subset or 'fpspeed' in FLAGS.spec17_subset:
    # AWS machines that advertise 16 GB have slightly less than that
    if available_memory < 15.6 * KB_TO_GB_MULTIPLIER:
      raise errors.Config.InvalidValue(
          'Available memory of %s GB is insufficient for spec17 speed runs.'
          % (available_memory / KB_TO_GB_MULTIPLIER)
      )
  if 'intrate' in FLAGS.spec17_subset or 'fprate' in FLAGS.spec17_subset:
    if available_memory < 2 * KB_TO_GB_MULTIPLIER:
      raise errors.Config.InvalidValue(
          'Available memory of %s GB is insufficient for spec17 rate runs.'
          % (available_memory / KB_TO_GB_MULTIPLIER)
      )


def Prepare(benchmark_spec):
  """Installs SPEC CPU2017 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  vms = benchmark_spec.vms
  background_tasks.RunThreaded(_Prepare, vms)


def _Prepare(vm):
  CheckVmPrerequisites(vm)
  vm.Install('speccpu2017')
  if 'ampere' in FLAGS.runspec_config:
    vm.Install('jemalloc')
  # Set attribute outside of the install function, so benchmark will work
  # even with --install_packages=False.
  config = speccpu2017.GetSpecInstallConfig(vm.GetScratchDir())
  setattr(vm, speccpu.VM_STATE_ATTR, config)
  _GenIncFile(vm)
  _GenNumactlIncFile(vm)


def _GenNumactlIncFile(vm):
  """Generates numactl.inc file."""
  config = speccpu2017.GetSpecInstallConfig(vm.GetScratchDir())
  if FLAGS.spec17_numa_bind_config:
    remote_numactl_path = os.path.join(config.spec_dir, 'config', 'numactl.inc')
    vm.Install('numactl')
    if FLAGS.spec17_numa_bind_config == 'auto':
      # Upload config and rename
      numa_bind_cfg = [
          'intrate,fprate:',
          'submit = echo "${command}" > run.sh ; $BIND bash run.sh',
      ]
      for idx in range(vm.NumCpusForBenchmark()):
        numa_bind_cfg.append(
            f'bind{idx} = /usr/bin/numactl --physcpubind=+{idx} --localalloc'
        )
      vm_util.CreateRemoteFile(
          vm, '\n'.join(numa_bind_cfg), remote_numactl_path
      )
    else:
      vm.PushFile(
          data.ResourcePath(FLAGS.spec17_numa_bind_config), remote_numactl_path
      )
    vm.RemoteCommand(f'cat {remote_numactl_path}')
  else:
    cfg_file_path = getattr(vm, speccpu.VM_STATE_ATTR, config).cfg_file_path
    vm.RemoteCommand(f'sed -i "/include: numactl.inc/d" {cfg_file_path}')


def _GenIncFile(vm):
  """Generates .inc files when not already included in tarballs."""
  if re.search(
      r'amd_(speed|rate)_aocc300_milan_(A1|B2).cfg', FLAGS.runspec_config
  ):
    # python script requires stdin
    vm.RemoteCommand("printf 'yes\nyes\nyes\n' > yes.txt")
    config = FLAGS.runspec_config.split('.')[0]
    cmd = (
        f'cd /scratch/cpu2017 && sudo ./run_{config}.py '
        '--tuning=base --exit_after_inc_gen < ~/yes.txt'
    )
    stdout, _ = vm.RemoteCommand(cmd)
    if 'ERROR' in stdout:
      raise errors.Benchmarks.PrepareException(
          'Error during creation of .inc file.'
          ' This is likely due to a missing entry in cpu_info.json for the'
          ' given machine type and speccpu17 tar file. This will cause the'
          ' run to fail.'
      )


def Run(benchmark_spec):
  """Runs SPEC CPU2017 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of lists of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  samples = []
  grouped_samples = background_tasks.RunThreaded(_Run, vms)

  for samples_list in grouped_samples:
    samples.extend(samples_list)

  return samples


def _OverwriteGccOptimize(vm):
  """Update OPTIMIZE flag if compiler is gcc."""
  if SPEC17_GCC_FLAGS.value is None:
    return
  config = speccpu2017.GetSpecInstallConfig(vm.GetScratchDir())
  config_filepath = getattr(vm, speccpu.VM_STATE_ATTR, config).cfg_file_path
  cmd = f"sed -Ei 's/i(\\s+OPTIMIZE\\s+=).*/\\1 {SPEC17_GCC_FLAGS.value}/' "
  cmd += config_filepath
  vm.RemoteCommand(cmd)
  return


def _Run(vm):
  """See base method.

  Args:
    vm: The vm to run the benchmark on.

  Returns:
    A list of sample.Sample objects.
  """

  # Make changes e.g. compiler flags to spec config file.
  if 'gcc' in FLAGS.runspec_config:
    # OPTIMIZE applies to all compilers, but our flag is only for gcc for
    # legacy reasons.
    _OverwriteGccOptimize(vm)

  # swap only if necessary; free local node memory and avoid remote memory;
  # reset caches; set stack size to unlimited
  # Also consider setting enable_transparent_hugepages flag to true
  cmd = (
      'echo 1 | sudo tee /proc/sys/vm/swappiness && '
      'echo 1 | sudo tee /proc/sys/vm/zone_reclaim_mode && '
      'sync ; echo 3 | sudo tee /proc/sys/vm/drop_caches && '
      'ulimit -s unlimited && '
  )

  cmd += 'runcpu '
  if FLAGS.spec17_build_only:
    cmd += '--action build '
  if FLAGS.spec17_rebuild:
    cmd += '--rebuild '

  version_specific_parameters = []
  copies = FLAGS.spec17_copies or vm.NumCpusForBenchmark()
  version_specific_parameters.append(f' --copies={copies} ')
  version_specific_parameters.append(
      ' --threads=%s ' % (FLAGS.spec17_threads or vm.NumCpusForBenchmark())
  )
  version_specific_parameters.append(
      ' --define build_ncpus=%s ' % (vm.NumCpusForBenchmark())
  )

  if FLAGS.spec17_fdo:
    version_specific_parameters.append('--feedback ')
    vm.RemoteCommand('cd /scratch/cpu2017; mkdir fdo_profiles')

  start_time = time.time()
  stdout, _ = speccpu.Run(
      vm, cmd, ' '.join(FLAGS.spec17_subset), version_specific_parameters
  )

  if not FLAGS.spec17_best_effort:
    if 'Error' in stdout and 'Please review this file' in stdout:
      raise errors.Benchmarks.RunError('Error during SPEC compilation.')

  if FLAGS.spec17_build_only:
    return [
        sample.Sample(
            'compilation_time',
            time.time() - start_time,
            's',
            {
                'spec17_subset': FLAGS.spec17_subset,
                'gcc_version': build_tools.GetVersion(vm, 'gcc'),
            },
        )
    ]

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

  for log_file in log_files:
    vm.RemoteCommand(
        f'cp {vm.GetScratchDir()}/cpu2017/result/{log_file} ~/{log_file}.log'
    )
    vm.PullFile(vm_util.GetTempDir(), f'~/{log_file}.log')

  samples = speccpu.ParseOutput(vm, log_files, partial_results, None)
  for item in samples:
    item.metadata['vm_name'] = vm.name
    item.metadata['spec17_gcc_flags'] = SPEC17_GCC_FLAGS.value or 'default'
    item.metadata['spec17_numa_bind_config'] = FLAGS.spec17_numa_bind_config
    item.metadata['spec17_copies'] = copies

  return samples


def Cleanup(benchmark_spec):
  """Cleans up SPEC CPU2017 from the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  vms = benchmark_spec.vms
  background_tasks.RunThreaded(speccpu.Uninstall, vms)
