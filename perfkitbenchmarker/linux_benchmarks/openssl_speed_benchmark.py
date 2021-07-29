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
"""Runs openssl speed.

Manual page:
https://www.openssl.org/docs/manmaster/man1/openssl-speed.html.
"""

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'openssl_speed'
BENCHMARK_CONFIG = """
openssl_speed:
  description: >
      Runs openssl-speed.
  vm_groups:
    default:
      vm_spec: *default_single_core
"""

FLAGS = flags.FLAGS
_OPENSSL_SPEED_DURATION = flags.DEFINE_integer(
    'openssl_speed_duration', 60, 'Duration of speed test in seconds.')
_OPENSSL_SPEED_ALGORITHM = flags.DEFINE_string(
    'openssl_speed_algorithm', 'aes-256-ctr',
    'Use the specified cipher or message digest algorithm.')
_OPENSSL_SPEED_MULTI = flags.DEFINE_integer(
    'openssl_speed_multi', None, 'Run multiple operations in parallel. '
    'By default, equals to number of vCPUs available for benchmark.')
# TODO(user): Support additional options.

# Block sizes for encryption/decryption. Openssl speed loop through following
# block sizes and measure how fast system able to encrypt/decrypt.
BLOCKSIZES_IN_BYTES = [16, 64, 256, 1024, 8192, 16384]


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  del benchmark_spec


def ParseOpenSSLOutput(raw_result: str, version: str, parallelism: int):
  """Parse output from openssl speed and return as samples."""
  matches = regex_util.ExtractExactlyOneMatch(r'evp\s+(.*)', raw_result).split()
  results = []
  for idx, blocksize in enumerate(BLOCKSIZES_IN_BYTES):
    value_unit_tuple = regex_util.ExtractExactlyOneMatch(
        r'([\d\.]+)(\w+)', matches[idx])
    metadata = {
        'duration': _OPENSSL_SPEED_DURATION.value,
        'algorithm': _OPENSSL_SPEED_ALGORITHM.value,
        'parallelism': parallelism,
        'version': version,
        'blocksize': blocksize
    }
    results.append(
        sample.Sample('Throughput', float(value_unit_tuple[0]),
                      value_unit_tuple[1], metadata))
  return results


def Run(benchmark_spec):
  """Run openssl-speed on the target vm.

  Sample output:
  OpenSSL 1.1.1k  25 Mar 2021
  built on: Thu Mar 25 20:49:34 2021 UTC
  options:bn(64,64) rc4(16x,int) des(int) aes(partial) blowfish(ptr)
  compiler: gcc -fPIC -pthread -m64 -Wa ...
  evp 730303.56k 2506149.08k 4473725.34k 5640335.56k 6048576.31k 6107063.91k

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample object.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  stderr, _ = vm.RemoteCommand('openssl version')
  version = regex_util.ExtractGroup(r'OpenSSL\s+([\w\.]+)\s+', stderr)
  parallelism = _OPENSSL_SPEED_MULTI.value or vm.NumCpusForBenchmark()
  raw_result, _ = vm.RemoteCommand('openssl speed -elapsed '
                                   f'-seconds {_OPENSSL_SPEED_DURATION.value} '
                                   f'-evp {_OPENSSL_SPEED_ALGORITHM.value} '
                                   f'-multi {parallelism}')

  return ParseOpenSSLOutput(raw_result, version, parallelism)


def Cleanup(benchmark_spec):
  """Cleanup openssl on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  del benchmark_spec
