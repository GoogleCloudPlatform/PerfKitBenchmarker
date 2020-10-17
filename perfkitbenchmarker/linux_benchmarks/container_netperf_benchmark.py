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

"""Runs netperf between containers.

docs:
http://www.netperf.org/svn/netperf2/tags/netperf-2.4.5/doc/netperf.html#TCP_005fRR
manpage: http://manpages.ubuntu.com/manpages/maverick/man1/netperf.1.html

Runs TCP_STREAM benchmark from netperf between two containers.
"""

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker.linux_benchmarks import netperf_benchmark

FLAGS = flags.FLAGS

# We set the default to 128KB (131072 bytes) to override the Linux default
# of 16K so that we can achieve the "link rate".
flags.DEFINE_integer('container_netperf_tcp_stream_send_size_in_bytes', 131072,
                     'Send size to use for TCP_STREAM tests (netperf -m flag)')

BENCHMARK_NAME = 'container_netperf'
BENCHMARK_CONFIG = """
container_netperf:
  description: Run netperf between containers.
  container_specs:
    netperf:
      image: netperf
      cpus: 2
      memory: 4GiB
  container_registry: {}
  container_cluster:
    vm_count: 2
    vm_spec:
      AWS:
        zone: us-east-1a
        machine_type: c5.xlarge
      Azure:
        zone: westus
        machine_type: Standard_D3_v2
      GCP:
        machine_type: n1-standard-4
        zone: us-west1-a

"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Start the netserver container.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  cluster = benchmark_spec.container_cluster
  cluster.DeployContainer('netperf', benchmark_spec.container_specs['netperf'])


def Run(benchmark_spec):
  """Run netperf TCP_STREAM between containers.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  samples = []
  cluster = benchmark_spec.container_cluster
  container_0 = cluster.containers['netperf'][0]
  spec = benchmark_spec.container_specs['netperf']
  spec.command = ['netperf',
                  '-t', 'TCP_STREAM',
                  '-H', container_0.ip_address,
                  '-l', '100',
                  '--',
                  '-m', FLAGS.container_netperf_tcp_stream_send_size_in_bytes,
                  '-o', netperf_benchmark.OUTPUT_SELECTOR]
  cluster.DeployContainer('netperf', benchmark_spec.container_specs['netperf'])
  container_1 = cluster.containers['netperf'][1]
  container_1.WaitForExit()
  throughput_sample, _, _ = netperf_benchmark.ParseNetperfOutput(
      container_1.GetLogs(), {}, 'TCP_STREAM', False)
  samples.append(throughput_sample)
  return samples


def Cleanup(unused_benchmark_spec):
  """Cleanup netperf.

  Args:
    unused_benchmark_spec: The benchmark specification. Contains all data that
        is required to run the benchmark.
  """
  pass
