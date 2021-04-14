# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Run GPU PingPong benchmarks.

This GPU pingpong benchmark aims to test the latency between 2 GPU in 2 VMs
which run TensorFlow gPRC servers.
The latency contains the ping phrase time and pong phase time.
"""

from typing import Any, Dict, List, Tuple
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.linux_packages import cuda_toolkit

_ENV = flags.DEFINE_string(
    'gpu_pingpong_env', 'PATH=/opt/conda/bin:$PATH', 'Env')

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'gpu_pingpong'
BENCHMARK_CONFIG = """
gpu_pingpong:
  description: Runs GPU PingPong Benchmark.
  vm_groups:
    default:
      vm_count: 2
      vm_spec:
        GCP:
          machine_type: a2-highgpu-8g
          zone: us-central1-c
          image_family: tf2-2-4-cu110-debian-10
          image_project: deeplearning-platform-release
        AWS:
          machine_type: p4d.24xlarge
          zone: us-east-1a
          image: ami-0e956fe81fa11d0a9
        Azure:
          machine_type: Standard_ND40rs_v2
          zone: eastus
          image: microsoft-dsvm:ubuntu-1804:1804-gen2:21.01.21
"""
_TEST_SCRIPT = 'gpu_pingpong_test.py'
_SERVER_SCRIPT = 'gpu_pingpong_server.py'
_SERVER_ADDR = '{hostname}:{port}'
_PORT = '2000'
_TIMELINE_PING = (r'timeline_label: "\[4B\] \[([\d\.]+)Mb/s\] \S+ from '
                  '/job:worker/replica:0/task:0/device:GPU:0 to '
                  '/job:worker/replica:0/task:1/device:GPU:0"')
_TIMELINE_PONG = (r'timeline_label: "\[4B\] \[([\d\.]+)Mb/s\] \S+ from '
                  '/job:worker/replica:0/task:1/device:GPU:0 to '
                  '/job:worker/replica:0/task:0/device:GPU:0"')


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Install and set up GPU PingPong on the target vm.

  Args:
    bm_spec: The benchmark specification
  """
  for vm in bm_spec.vms:
    vm.RemoteCommand(f'{_ENV.value} pip install absl-py')
    vm.AuthenticateVm()
    for script in [_TEST_SCRIPT, _SERVER_SCRIPT]:
      vm.PushFile(data.ResourcePath(script), script)
  server1, server2 = bm_spec.vms[0], bm_spec.vms[1]
  server1_addr = _SERVER_ADDR.format(
      hostname=server1.hostname, port=_PORT)
  server2_addr = _SERVER_ADDR.format(
      hostname=server2.hostname, port=_PORT)
  server1.RemoteCommand(f'{_ENV.value} nohup python {_SERVER_SCRIPT} '
                        f'{server1_addr} {server2_addr} 0 '
                        '1> /tmp/stdout.log 2> /tmp/stderr.log &')
  server2.RemoteCommand(f'{_ENV.value} nohup python {_SERVER_SCRIPT} '
                        f'{server1_addr} {server2_addr} 1 '
                        '1> /tmp/stdout.log 2> /tmp/stderr.log &')


def _RunGpuPingpong(vm: virtual_machine.BaseVirtualMachine,
                    addr: str) -> List[Tuple[float, float]]:
  """Returns the Ping and Pong latency times."""
  stdout, stderr = vm.RemoteCommand(
      f'{_ENV.value} python {_TEST_SCRIPT} {addr}')
  ping_bws = [float(bw) for bw in
              regex_util.ExtractAllMatches(_TIMELINE_PING, stdout + stderr)]
  pong_bws = [float(bw) for bw in
              regex_util.ExtractAllMatches(_TIMELINE_PONG, stdout + stderr)]

  return list(zip(ping_bws, pong_bws))


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Run GPU PingPong test.

  It tests the latency between 2 GPU in 2 VMs using TensorFlow gPRC server which
  were started during prepare phrase.

  Args:
    bm_spec: The benchmark specification

  Returns:
    A list of sample.Sample objects.
  """
  client_vm, server_vm = bm_spec.vms
  server_address = _SERVER_ADDR.format(hostname=server_vm.hostname, port=_PORT)
  base_metadata = cuda_toolkit.GetMetadata(client_vm)
  samples = []

  bws = _RunGpuPingpong(client_vm, server_address)
  for ping_bw, pong_bw in bws[1:]:
    metadata = {'ping': 32 / ping_bw, 'pong': 32 / pong_bw}
    metadata.update(base_metadata)
    samples.append(sample.Sample(
        'latency', 32 / ping_bw + 32 / pong_bw, 'microseconds',
        metadata))
  return samples


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Cleanup GPU PingPong on the cluster.

  Args:
    bm_spec: The benchmark specification. Contains all data that
      is required to run the benchmark.
  """
  del bm_spec
