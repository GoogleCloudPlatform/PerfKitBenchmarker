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
"""Module containing DCPerf TaoBench.

Runs standalone mode on one VM, or autoscale mode with separate client VMs.
"""

import json
import shlex

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.linux_packages import dcperf

_STANDALONE_JOB = 'tao_bench_standalone'
_SERVER_JOB = 'tao_bench_autoscale'
_CLIENT_JOB = 'tao_bench_custom'
_PORT = 11211
_WARMUP_TIME = flags.DEFINE_integer(
    'taobench_warmup_time', 1700, 'TaoBench autoscale warmup time in seconds.'
)
_TEST_TIME = flags.DEFINE_integer(
    'taobench_test_time', 720, 'TaoBench autoscale test time in seconds.'
)
_METRICS = {
    'total_qps': 'qps',
    'fast_qps': 'qps',
    'slow_qps': 'qps',
    'hit_ratio': '',
    'num_data_points': '',
    'score': '',
}


def Install(vm: virtual_machine.BaseVirtualMachine) -> None:
  vm.Install('dcperf')
  if vm.OS_TYPE == os_types.UBUNTU2604:  # pyrefly: ignore[missing-argument]
    _ApplyUbuntu2604Workarounds(vm)
  vm.RobustRemoteCommand(
      f'cd {dcperf.DCPERF_DIR} && '
      f'sudo ./benchpress_cli.py install {_STANDALONE_JOB}'
  )
  # Large VMs otherwise run out of ephemeral ports.
  vm.RemoteCommand(
      "sudo sysctl -w net.ipv4.ip_local_port_range='1024 65535' "
      'net.ipv4.tcp_tw_reuse=1'
  )


def _ApplyUbuntu2604Workarounds(vm: virtual_machine.BaseVirtualMachine) -> None:
  """Patches DCPerf v1.0's TaoBench install scripts to build on Ubuntu 26.04."""
  # Otherwise folly's getdeps builds ninja 1.10.2, which needs Python's
  # removed pipes module.
  vm.InstallPackages('ninja-build')
  edits = (
      # CMake 4 rejects old cmake_minimum_required versions.
      '1a export CMAKE_POLICY_VERSION_MINIMUM=3.5',
      # `uname -p` prints "unknown".
      's/uname -p/uname -m/',
      # The bundled libevent calls arc4random_addrandom, which glibc lacks.
      (
          's|pushd libevent/|& \\&\\& '
          'sed -i "s/arc4random_addrandom(/(void)(/" evutil_rand.c|'
      ),
      # PCRE1 is gone. memtier's configure requires it but memtier doesn't
      # use it.
      's/ libpcre3 libpcre3-dev//',
      (
          's|autoreconf --force --install|'
          'sed -i "/pcre_compile/d" configure.ac \\&\\& &|'
      ),
      # Boost.Python doesn't build against NumPy 2, and nothing uses it.
      (
          's|^git checkout v2024.06.24.00|& \\&\\& '
          'sed -i "/--with-python/d" build/fbcode_builder/manifests/boost|'
      ),
      # folly's tests don't link against the system glog.
      's|getdeps.py --allow-system-packages build|& --no-tests|',
      # GCC 15 enables _GLIBCXX_ASSERTIONS, which aborts the server when it
      # draws a random number from an empty range during warmup.
      (
          's|0008-tao_bench_count_nanosleeps.diff"|& \\&\\& sed -i '
          '"s/n_max) {$/\\& if (n_min >= n_max) return n_min;/" db_items.cpp|'
      ),
      # The client's key prefix isn't NUL-terminated and the heap past it isn't
      # zero, so keys get garbage suffixes and the cache never warms up.
      (
          's|0005-tao_bench_client_memtier_20230615.diff"|& \\&\\& sed -i'
          ' "s/malloc(sizeof(char) \\\\* cfg.key_bytes)/calloc(cfg.key_bytes,'
          ' 1)/" memtier_benchmark.cpp|'
      ),
  )
  sed_args = ' '.join(f'-e {shlex.quote(edit)}' for edit in edits)
  vm.RemoteCommand(
      f'sed -i {sed_args} '
      f'{dcperf.DCPERF_DIR}/packages/tao_bench/install_tao_bench*.sh'
  )


def Run(
    server: virtual_machine.BaseVirtualMachine,
    clients: list[virtual_machine.BaseVirtualMachine] | None = None,
) -> list[sample.Sample]:
  """Runs standalone mode, or autoscale mode if there are clients."""
  if clients:
    output = _RunAutoscale(server, clients)
  else:
    output, _ = server.RobustRemoteCommand(
        f'cd {dcperf.DCPERF_DIR} && '
        f'sudo ./benchpress_cli.py run {_STANDALONE_JOB}'
    )
  report = output.rpartition('Results Report:')[2].strip().splitlines()[0]
  metrics = json.loads(report)['metrics']
  if metrics['successful_instances'] != metrics['spawned_instances']:
    raise errors.Benchmarks.RunError(f'TaoBench servers failed: {metrics}')
  if metrics['num_data_points'] == 0:
    raise errors.Benchmarks.RunError('TaoBench reported no valid datapoints.')
  return [
      sample.Sample(name, metrics[name], unit)
      for name, unit in _METRICS.items()
  ]


def _RunAutoscale(
    server: virtual_machine.BaseVirtualMachine,
    clients: list[virtual_machine.BaseVirtualMachine],
) -> str:
  """Runs the server in the background and the clients against it."""
  memsize = 2 * server.num_cpus
  times = {'warmup_time': _WARMUP_TIME.value, 'test_time': _TEST_TIME.value}
  # One server, not DCPerf's default of one per 72 vCPUs.
  server_input = json.dumps({'num_servers': 1, 'memsize': memsize} | times)
  client_input = json.dumps(
      {'server_hostname': server.internal_ip, 'server_memsize': memsize} | times
  )
  server.RemoteCommand(
      f'cd {dcperf.DCPERF_DIR} && {{ nohup sudo ./benchpress_cli.py run '
      f"{_SERVER_JOB} -i '{server_input}' > ~/server.log 2>&1 & "
      'echo $! > ~/server.pid; }'
  )
  server.RemoteCommand(
      f'while ! nc -z localhost {_PORT}; do sleep 1; done', timeout=600
  )
  background_tasks.RunThreaded(
      lambda client: client.RobustRemoteCommand(
          f'cd {dcperf.DCPERF_DIR} && sudo ./benchpress_cli.py run '
          f"{_CLIENT_JOB} -r client -i '{client_input}'"
      ),
      clients,
  )
  output, _ = server.RobustRemoteCommand(
      'tail --pid=$(cat ~/server.pid) -f /dev/null; cat ~/server.log'
  )
  return output
