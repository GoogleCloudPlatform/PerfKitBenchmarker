# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
from perfkitbenchmarker import errors, context


class BaseVPN(object):
  """An object representing the Base VPN GW."""
  GW_PAIR = None  # pair of vpngw's to create tunnel between
  name = None  # name of the vpn created

  @classmethod
  def GetVPN(cls):
    """Returns a BaseVPN.
    This method is used instead of directly calling the class's constructor.
    It creates BaseVPN instances and registers them.
    If a BaseVPN object has already been registered, that object
    will be returned rather than creating a new one. This enables multiple
    VMs to call this method and all share the same BaseVPN object.
    """
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('GetVPN called in a thread without a '
                         'BenchmarkSpec.')
    with benchmark_spec.vpns_lock:
      key = cls.CLOUD
      if key not in benchmark_spec.vpns:
        benchmark_spec.vpngws[key] = cls()
      return benchmark_spec.vpngws[key]


class VPNService(object):
  CLOUD = None

  def __init__(self):
    pass

  def Create(self):
    raise NotImplementedError

  def Destroy(self):
    raise NotImplementedError

  def Flush(self):
    raise NotImplementedError

  def GetHosts(self):
    raise NotImplementedError

  def GetMetadata(self):
    raise NotImplementedError
