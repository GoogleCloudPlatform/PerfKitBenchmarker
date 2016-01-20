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

"""Records the NTP offset of a VM."""

import logging
import time

import perfkitbenchmarker
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import ntp


BENCHMARK_NAME = 'ntp_benchmark'
BENCHMARK_CONFIG = """
ntp_benchmark:
  description: check NTP offset
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: 1
"""

FLAGS = flags.FLAGS


flag_util.DEFINE_units(
    'ntp_total_time',
    perfkitbenchmarker.UNIT_REGISTRY.parse_expression('10min'),
    "The length of time to measure the VM's NTP properties.",
    convertible_to=perfkitbenchmarker.UNIT_REGISTRY.second)

flag_util.DEFINE_units(
    'ntp_measurement_frequency',
    perfkitbenchmarker.UNIT_REGISTRY.parse_expression('10sec'),
    "How frequently to query the VM's NTP daemon.",
    convertible_to=perfkitbenchmarker.UNIT_REGISTRY.second)


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  pass


def Run(benchmark_spec):
  """Measure the clock performance of all VMs.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects with individual machine NTP offsets.
  """
  vm = benchmark_spec.vms[0]

  # We don't use the ntp.StartNTP() convenience function because we
  # want to measure the time until the first sync.
  vm.firewall.AllowPort(vm, 123)
  vm.Install('ntp')
  ntp.WaitForNTPDaemon(vm)

  before_sync = time.time()
  ntp.WaitForNTPSync(vm)
  after_sync = time.time()

  samples = [sample.Sample('ntp-time-to-first-sync',
                           after_sync - before_sync, 'sec')]

  seconds_unit = perfkitbenchmarker.UNIT_REGISTRY.seconds
  total_time = FLAGS.ntp_total_time.to(seconds_unit).magnitude
  measurement_frequency = (
      FLAGS.ntp_measurement_frequency.to(seconds_unit).magnitude)

  start_time = time.time()
  current_time = start_time
  while current_time - start_time < total_time:
    off = ntp.GetNTPTimeOffset(vm)
    samples.append(sample.Sample('ntp-offset', off, 'sec'))

    wander = ntp.GetNTPClockWander(vm)
    samples.append(sample.Sample('ntp-wander', wander, 'ppm'))

    since_sync = ntp.SecsSinceLastSync(vm)
    if since_sync is not None:
      samples.append(sample.Sample('ntp-time-since-sync', since_sync, 'sec'))

    time.sleep(measurement_frequency)
    current_time = time.time()

  logging.info(samples)
  return samples


def Cleanup(unused_benchmark_spec):
  pass
