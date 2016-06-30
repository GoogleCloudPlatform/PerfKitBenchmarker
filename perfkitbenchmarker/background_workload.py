# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing classes for background workloads."""

from perfkitbenchmarker import os_types
from perfkitbenchmarker import vm_util

BACKGROUND_WORKLOADS = []

BACKGROUND_IPERF_PORT = 20001
BACKGROUND_IPERF_SECONDS = 2147483647


class AutoRegisterBackgroundWorkloadMeta(type):
  """Metaclass which allows BackgroundWorkloads to be auto-registered."""

  def __init__(cls, name, bases, dct):
    super(AutoRegisterBackgroundWorkloadMeta, cls).__init__(name, bases, dct)
    BACKGROUND_WORKLOADS.append(cls)


class BaseBackgroundWorkload(object):
  """Baseclass for background workloads."""
  __metaclass__ = AutoRegisterBackgroundWorkloadMeta

  EXCLUDED_OS_TYPES = []

  @staticmethod
  def IsEnabled(vm):
    """Returns true if this background workload is enabled on this VM."""
    return False

  @staticmethod
  def Prepare(vm):
    """Prepares the background workload on this VM."""
    pass

  @staticmethod
  def Start(vm):
    """Starts the background workload on this VM."""
    pass

  @staticmethod
  def Stop(vm):
    """Stops the background workload on this VM."""
    pass


class CpuWorkload(BaseBackgroundWorkload):
  """Workload that runs sysbench in the background."""

  EXCLUDED_OS_TYPES = os_types.WINDOWS_OS_TYPES

  @staticmethod
  def IsEnabled(vm):
    """Returns true if this background workload is enabled on this VM."""
    return bool(vm.background_cpu_threads)

  @staticmethod
  def Prepare(vm):
    """Prepares the background workload on this VM."""
    vm.Install('sysbench')

  @staticmethod
  def Start(vm):
    """Starts the background workload on this VM."""
    vm.RemoteCommand(
        'nohup sysbench --num-threads=%s --test=cpu --cpu-max-prime=10000000 '
        'run 1> /dev/null 2> /dev/null &' % vm.background_cpu_threads)

  @staticmethod
  def Stop(vm):
    """Stops the background workload on this VM."""
    vm.RemoteCommand('pkill -9 sysbench')


class NetworkWorkload(BaseBackgroundWorkload):
  """Workload that runs iperf in the background."""

  EXCLUDED_OS_TYPES = os_types.WINDOWS_OS_TYPES

  @staticmethod
  def IsEnabled(vm):
    """Returns true if this background workload is enabled on this VM."""
    return bool(vm.background_network_mbits_per_sec)

  @staticmethod
  def Prepare(vm):
    """Prepares the background workload on this VM."""
    vm.Install('iperf')

  @staticmethod
  def Start(vm):
    """Starts the background workload on this VM."""
    vm.AllowPort(BACKGROUND_IPERF_PORT)
    vm.RemoteCommand('nohup iperf --server --port %s &> /dev/null &' %
                     BACKGROUND_IPERF_PORT)
    stdout, _ = vm.RemoteCommand('pgrep iperf -n')
    vm.server_pid = stdout.strip()

    if vm.background_network_ip_type == vm_util.IpAddressSubset.EXTERNAL:
      ip_address = vm.ip_address
    else:
      ip_address = vm.internal_ip
    iperf_cmd = ('nohup iperf --client %s --port %s --time %s -u -b %sM '
                 '&> /dev/null &' % (ip_address, BACKGROUND_IPERF_PORT,
                                     BACKGROUND_IPERF_SECONDS,
                                     vm.background_network_mbits_per_sec))

    vm.RemoteCommand(iperf_cmd)
    stdout, _ = vm.RemoteCommand('pgrep iperf -n')
    vm.client_pid = stdout.strip()

  @staticmethod
  def Stop(vm):
    """Stops the background workload on this VM."""
    vm.RemoteCommand('kill -9 ' + vm.client_pid)
    vm.RemoteCommand('kill -9 ' + vm.server_pid)
