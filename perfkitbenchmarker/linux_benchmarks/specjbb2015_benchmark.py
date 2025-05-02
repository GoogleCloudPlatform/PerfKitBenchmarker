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
"""Runs the Spec JBB 2015 benchmark https://www.spec.org/jbb2015/.

User guide: https://www.spec.org/jbb2015/docs/userguide.pdf.
"""
import re

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import numactl
from perfkitbenchmarker.linux_packages import openjdk
from perfkitbenchmarker.linux_packages import openjdk_neoverse
from perfkitbenchmarker.linux_packages import specjbb

BENCHMARK_NAME = 'specjbb2015'
BENCHMARK_CONFIG = """
specjbb2015:
  description: Run specjbb2015
  vm_groups:
    default:
      vm_spec: *default_dual_core
      disk_spec: *default_50_gb
  flags:
    openjdk_version: 21
    enable_transparent_hugepages: True
"""

FLAGS = flags.FLAGS
_FOUR_HOURS = 60 * 60 * 4
_DEFAULT_JVM_ARGS = ''
_DEFAULT_JVM_CONT_TXI_ARGS = (
    '-Xms2g -Xmx2g -Xmn1536m -XX:+AlwaysPreTouch -XX:ParallelGCThreads=2'
)
_DEFAULT_COMPOSITE_MEMORY_RATIO = 0.8
_DEFAULT_WORKERS_RATIO = 1.0
_DEFAULT_NUM_GROUPS = 1
_DEFAULT_MEMORY_RATIO = 0.75
_SPEC_JBB_2015_ISO = specjbb.SPEC_JBB_2015_ISO
_SPEC_DIR = specjbb.SPEC_DIR
_LOG_FILE = '~/specjbb2015.log'
_JAR_FILE = 'specjbb2015.jar'
_PROPS_FILE = 'config/specjbb2015.props'
BENCHMARK_DATA = {
    _SPEC_JBB_2015_ISO: (
        '524bc1588a579ddf35cfada5e07a408c78b5939e72ee5f02b05422d5c0d214bd'
    )
}
BACKEND_MODE = 'backend'
MULTIJVM_MODE = 'MultiJVM'
COMPOSITE_MODE = 'COMPOSITE'
MULTICONTROLLER_MODE = 'multicontroller'
TXINJECTOR_MODE = 'txinjector'
NEW_MAX_RATIO = 0.94  # Taken from customer script

flags.DEFINE_float(
    'specjbb_workers_ratio',
    _DEFAULT_WORKERS_RATIO,
    'A number indicating number of workers per vCPU per group.',
)
flags.DEFINE_enum(
    'specjbb_run_mode',
    MULTIJVM_MODE,
    [MULTIJVM_MODE, COMPOSITE_MODE],
    'String representing run mode. COMPOSITE or MultiJVM.',
)
flags.DEFINE_boolean(
    'specjbb_auto_groups',
    True,
    'Used in MultiJVM. If true, sets groups equal to NUMA nodes and overrides '
    'specjbb_num_groups.',
)
flags.DEFINE_integer(
    'specjbb_num_groups',
    _DEFAULT_NUM_GROUPS,
    'Used in MultiJVM, number of groups.',
)
flags.DEFINE_float(
    'specjbb_memory_ratio',
    _DEFAULT_MEMORY_RATIO,
    'Used in MultiJVM, is overridden by specjbb_ram_mb_per_core. '
    'This value determines the total memory usage of the JVM backends by '
    "multplying with the VM's memory. For example, a memory ratio of 0.75 "
    'and a machine with 64 GB memory and 2 backend groups means that each '
    'backend will have 24 GB memory allotted.',
)
flags.DEFINE_integer(
    'specjbb_ram_mb_per_core',
    None,
    'Used in MultiJVM. Setting this to 1500 means that the total memory usage '
    'of the JVM backends will be num_cpus * 1.5 GB. This is useful for '
    'obtaining a comparable performance number between machines with '
    'different memory ratios, e.g. 1 vCPU : 2 GB and 1 vCPU : 8 GB, or '
    'AWS EC2 C/M/R and GCE highcpu/standard/highmem. '
    'Overrides specjbb_memory_ratio if set',
)
flags.DEFINE_bool(
    'specjbb_numa_aware',
    True,
    'Whether to have MultiJVM backends pinned to specific NUMA nodes.',
)
flags.DEFINE_bool(
    'build_openjdk_neoverse',
    False,
    'Whether to build OpenJDK optimized for ARM Neoverse.'
    'Requires Ubuntu 1804 and OpenJDK 11.',
)
flag_util.DEFINE_integerlist(
    'specjbb_multijvm_nodes',
    None,
    'By default specjbb jvm groups are bound to all numa nodes in '
    'host in a round robin way. When specjbb_multijvm_nodes is specified, '
    'specjbb jvm groups are bound to the specified numa nodes in a '
    'round robin way. For example, if user want to bind 4 specjbb '
    'jvm groups to numa node 0 and 1 in the following order: 1, 0, '
    '1, 0, user can specify the specjbb_multijvm_nodes flag as following:'
    '--specjbb_multijvm_nodes=1,0',
)
flags.DEFINE_integer(
    'specjbb_file_descriptors_limit',
    64 * 1024,
    'Set FD limit for specjbb backend java processes. Specjbb '
    'background processes tends to create a lot of network connections. '
    'It could fail if FD limit is too low. Please search for "Too many '
    'open files" in the web page for detailed info: '
    'http://spec.org/jbb2015/docs/knownissues.html'
    'Please be careful when setting this flag. its value should be '
    'smaller than sysctl flag fs.nr_open. Benchmark will fail if this '
    'flag is set to a value bigger than fs.nr_open.',
    lower_bound=0,
)
flags.DEFINE_integer(
    'specjbb_connection_pool_size',
    256,
    'User can use this flag to set connection pool size for all specjbb '
    'agents. Please refer to section 16.1 of specjbb userguide:'
    'https://www.spec.org/jbb2015/docs/userguide.pdf',
    lower_bound=0,
)


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install Specjbb2015 on the target vm.

  Args:
    benchmark_spec: The benchmark specification.
  """
  vm = benchmark_spec.vms[0]

  vm.Install('specjbb')

  vm.Install('openjdk')

  # Used on m6g (AWS Graviton 2) machines for optimal performance
  if FLAGS.build_openjdk_neoverse:
    openjdk_neoverse.InstallNeoverseCompiledOpenJDK(
        vm, openjdk.OPENJDK_VERSION.value
    )
  vm.InstallPackages('numactl')

  # swap only if necessary; free local node memory and avoid remote memory;
  # reset caches; set stack size to unlimited
  # Also consider setting enable_transparent_hugepages flag to true
  cmd = (
      'echo 1 | sudo tee /proc/sys/vm/swappiness && '
      'echo 1 | sudo tee /proc/sys/vm/zone_reclaim_mode && '
      'sync ; echo 3 | sudo tee /proc/sys/vm/drop_caches && '
      'ulimit -s unlimited'
  )
  vm.RemoteCommand(cmd)


def _GetNumGroups(vm):
  """Returns integer number of backend groups to use for MultiJVM."""
  if FLAGS.specjbb_auto_groups:
    return len(numactl.GetNuma(vm))
  return FLAGS.specjbb_num_groups


def _MaxHeapMB(vm, mode):
  """Returns max heap size in MB as an int."""
  if mode == BACKEND_MODE:
    if FLAGS.specjbb_ram_mb_per_core:  # overrides --specjbb_memory_ratio if set
      return (
          int(vm.NumCpusForBenchmark() // _GetNumGroups(vm))
          * FLAGS.specjbb_ram_mb_per_core
      )
    else:
      return int(
          vm.total_memory_kb
          * FLAGS.specjbb_memory_ratio
          // _GetNumGroups(vm)
          // 1024
      )
  elif mode == COMPOSITE_MODE:
    return int(vm.total_memory_kb * _DEFAULT_COMPOSITE_MEMORY_RATIO / 1024)


def _JVMArgs(vm, mode):
  """Determines JVM args and returns them as a string."""
  if mode in (TXINJECTOR_MODE, MULTICONTROLLER_MODE):
    return _DEFAULT_JVM_CONT_TXI_ARGS

  gc_size = int(vm.NumCpusForBenchmark() / _GetNumGroups(vm))
  jvm_backend_gc_arg = f'-XX:ParallelGCThreads={gc_size}'

  # Determine max/new heap arguments. max per group = 3/8 * vCPU GB.
  jvm_backend_mem_arg = '-Xms{max_}m -Xmx{max_}m -Xmn{new_}m '.format(
      max_=_MaxHeapMB(vm, BACKEND_MODE),
      new_=int(_MaxHeapMB(vm, BACKEND_MODE) * NEW_MAX_RATIO),
  )
  jvm_composite_mem_arg = '-Xms{max_}m -Xmx{max_}m -Xmn{new_}m '.format(
      max_=_MaxHeapMB(vm, COMPOSITE_MODE),
      new_=int(_MaxHeapMB(vm, COMPOSITE_MODE) * NEW_MAX_RATIO),
  )

  if mode == BACKEND_MODE:
    return ' '.join(
        [jvm_backend_gc_arg, jvm_backend_mem_arg, _DEFAULT_JVM_ARGS]
    )
  elif mode == COMPOSITE_MODE:
    return ' '.join([jvm_composite_mem_arg, _DEFAULT_JVM_ARGS])
  else:
    raise errors.Benchmarks.RunError('Invalid specjbb mode!')


def _SpecArgs(vm, mode):
  """Determines Spec args and returns them as a string."""

  num_workers = (
      vm.NumCpusForBenchmark() * FLAGS.specjbb_workers_ratio / _GetNumGroups(vm)
  )
  spec_num_workers_arg = f' -Dspecjbb.forkjoin.workers={int(num_workers)}'
  spec_num_groups_arg = f' -Dspecjbb.group.count={_GetNumGroups(vm)}'
  spec_rt_curve_arg = '-Dspecjbb.controller.rtcurve.warmup.step=0.5'
  spec_mr_arg = f'-Dspecjbb.mapreducer.pool.size={_GetNumGroups(vm) * 2}'
  spec_connect_pool_size_arg = f'-Dspecjbb.comm.connect.client.pool.size={FLAGS.specjbb_connection_pool_size}'

  if mode == TXINJECTOR_MODE:
    return ''
  elif mode == MULTICONTROLLER_MODE:
    return ' '.join([
        spec_rt_curve_arg,
        spec_mr_arg,
        spec_num_workers_arg,
        spec_num_groups_arg,
        spec_connect_pool_size_arg,
    ])
  elif mode == BACKEND_MODE:
    return ''
  elif mode == COMPOSITE_MODE:
    return spec_num_workers_arg
  else:
    raise errors.Benchmarks.RunError('Invalid specjbb mode!')


def _CollectSLAMetrics(vm):
  """Gathers SLA metrics from specjbb output files."""

  # The log file reports the location of the report.html file. Since date/time
  # are part of the report filename, we must determine it at runtime. The .raw
  # file is easier to parse than the .html file, so parse that instead.
  grep_stdout, _ = vm.RemoteCommand(
      "grep -oE '[^ ]+html' ~/specjbb2015.log", ignore_failure=True
  )
  file_prefix = grep_stdout.split('.')[0]
  filename = f'spec/{file_prefix}.raw'
  cmd = f'cat {filename} | grep SLA-'
  sla_stdout, _ = vm.RemoteCommand(cmd, ignore_failure=True)
  return sla_stdout


def ParseJbbOutput(stdout, metadata):
  """Generates samples from the RUN RESULT string."""

  samples = []
  regex = re.compile(
      r'RUN\sRESULT:.*?max\-jOPS\s=\s(?P<maxjops>\d+),\s+'
      r'critical-jOPS\s=\s(?P<crjops>\d+)'
  )

  jops = regex.search(stdout)
  if jops:
    samples.append(
        sample.Sample('max_jOPS', int(jops.group('maxjops')), 'jops', metadata)
    )
    samples.append(
        sample.Sample(
            'critical_jOPS', int(jops.group('crjops')), 'jops', metadata
        )
    )
  else:
    raise errors.Benchmarks.RunError('No specjbb results found!')

  return samples


def _RunBackgroundNumaPinnedCommand(vm, cmd_list, node_id):
  """In a shell session, cd and run a numa pinned background command.

  A user may opt to not NUMA pin with the specjbb_numa_aware flag.

  Args:
    vm: VM to run the command on
    cmd_list: list of commands to be joined together
    node_id: NUMA node to pin command on.
  """
  fd_limit_cmd = f'ulimit -n {FLAGS.specjbb_file_descriptors_limit}'
  if FLAGS.specjbb_numa_aware:
    # Persist the nohup command past the ssh session, and numa pin.
    # "sh -c 'cd /whereever; nohup ./whatever > /dev/null 2>&1 &'"
    # "numa --cpunodebind 0 --membind 0 cmd"
    cmd = (
        "sh -c '{fd_limit_cmd} && cd {dir} && nohup numactl "
        '--cpunodebind {node_id} '
        "--membind {node_id} {cmd} 2>&1 &'"
    ).format(
        fd_limit_cmd=fd_limit_cmd,
        node_id=node_id,
        dir=_SPEC_DIR,
        cmd=' '.join(cmd_list),
    )
  else:
    cmd = ("sh -c '{fd_limit_cmd} && cd {dir} && nohup {cmd} 2>&1 &'").format(
        fd_limit_cmd=fd_limit_cmd, dir=_SPEC_DIR, cmd=' '.join(cmd_list)
    )
  vm.RemoteCommand(cmd)


def Run(benchmark_spec):
  """Runs Specjbb2015 on the target vm.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects with the performance results.

  Raises:
    Benchmarks.RunError: If no results are found.
  """
  vm = benchmark_spec.vms[0]

  if FLAGS.specjbb_run_mode == MULTIJVM_MODE:
    if FLAGS.specjbb_multijvm_nodes:
      node_ids = FLAGS.specjbb_multijvm_nodes
    else:
      numa_map = numactl.GetNuma(vm)
      node_ids = list(numa_map.keys())

    # Run backends and txinjectors as background commands
    # java -jar specjbb2015.jar -m txinjector -G GRP1 -J JVM1 > grp1jvm1.log
    # java -jar specjbb2015.jar -m backend -G GRP1 -J JVM1 > grp1jvm2.log
    for group in range(1, _GetNumGroups(vm) + 1):
      node_id = node_ids[(group - 1) % len(node_ids)]

      txinjector_cmd = [
          'java',
          _JVMArgs(vm, TXINJECTOR_MODE),
          '-jar',
          _JAR_FILE,
          '-m',
          TXINJECTOR_MODE,
          '-G',
          f'GRP{group}',
          '-J',
          'JVM1',
          '>',
          f'grp{group}jvm1.log',
      ]
      _RunBackgroundNumaPinnedCommand(vm, txinjector_cmd, node_id)

      backend_cmd = [
          'java',
          _JVMArgs(vm, BACKEND_MODE),
          '-jar',
          _JAR_FILE,
          '-m',
          BACKEND_MODE,
          '-G',
          f'GRP{group}',
          '-J',
          'JVM2',
          '>',
          f'grp{group}jvm2.log',
      ]
      _RunBackgroundNumaPinnedCommand(vm, backend_cmd, node_id)

    # Run multicontroller as a foreground command
    controller_cmd = [
        'java',
        _JVMArgs(vm, MULTICONTROLLER_MODE),
        _SpecArgs(vm, MULTICONTROLLER_MODE),
        '-jar',
        _JAR_FILE,
        '-m',
        MULTICONTROLLER_MODE,
        '-p',
        _PROPS_FILE,
    ]
    run_cmd = ('cd {dir} && {cmd} 2>&1 | tee {log_file}').format(
        dir=_SPEC_DIR, cmd=' '.join(controller_cmd), log_file=_LOG_FILE
    )
    stdout, _ = vm.RobustRemoteCommand(run_cmd)
    max_heap_size_gb = _MaxHeapMB(vm, BACKEND_MODE) / 1000.0  # for metadata

  else:  # COMPOSITE mode
    run_cmd = [
        'java',
        _JVMArgs(vm, COMPOSITE_MODE),
        _SpecArgs(vm, COMPOSITE_MODE),
        '-jar',
        _JAR_FILE,
        '-m',
        COMPOSITE_MODE,
        '-p',
        _PROPS_FILE,
    ]
    cmd = ('cd {dir} && {cmd} 2>&1 | tee {log_file}').format(
        dir=_SPEC_DIR, cmd=' '.join(run_cmd), log_file=_LOG_FILE
    )
    stdout, _ = vm.RemoteCommand(cmd, timeout=_FOUR_HOURS)
    max_heap_size_gb = _MaxHeapMB(vm, COMPOSITE_MODE) / 1000.0  # for metadata

  jdk_metadata = FLAGS.openjdk_version
  if FLAGS.build_openjdk_neoverse:
    jdk_metadata += '_neoverse_optimized'

  metadata = {
      'OpenJDK_version': jdk_metadata,
      'iso_hash': BENCHMARK_DATA[_SPEC_JBB_2015_ISO],
      'num_workers': int(
          vm.NumCpusForBenchmark()
          * FLAGS.specjbb_workers_ratio
          / _GetNumGroups(vm)
      ),
      'num_groups': _GetNumGroups(vm),
      'worker_ratio': FLAGS.specjbb_workers_ratio,
      'memory_ratio': FLAGS.specjbb_memory_ratio,
      'ram_mb_per_core': FLAGS.specjbb_ram_mb_per_core,
      'max_heap_size': f'{max_heap_size_gb}g',
      'specjbb_mode': FLAGS.specjbb_run_mode,
      'sla_metrics': _CollectSLAMetrics(vm),
      'specjbb_numa_aware': FLAGS.specjbb_numa_aware,
  }
  vm.PullFile(vm_util.GetTempDir(), _LOG_FILE)
  return ParseJbbOutput(stdout, metadata)


def Cleanup(benchmark_spec):
  """Cleanup Specjbb2015 on the target vm.

  Args:
    benchmark_spec: The benchmark specification.
  """
  vm = benchmark_spec.vms[0]
  specjbb.Uninstall(vm)
