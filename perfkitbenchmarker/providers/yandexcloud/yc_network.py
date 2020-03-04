# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing classes related to YC networking.

The Network class allows VMs to communicate via internal ips and isolates
PerfKitBenchmarker VMs from others in the same folder.

See https://cloud.yandex.ru/docs/vpc/ formore information about YC networking.
"""

import threading

from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.yandexcloud import util
from perfkitbenchmarker import providers

FLAGS = flags.FLAGS
NETWORK_RANGE = '10.0.0.0/8'


class YcNetworkSpec(network.BaseNetworkSpec):

  def __init__(self, folder_id=None, **kwargs):
    """Initializes the YcNetworkSpec.

    Args:
      folder_id: The folder in which the Network should be created.
      kwargs: Additional key word arguments passed to BaseNetworkSpec.
    """
    super(YcNetworkSpec, self).__init__(**kwargs)
    self.folder_id = folder_id


class YcNetworkResource(resource.BaseResource):
  """Object representing a YC Network resource."""

  def __init__(self, name, folder_id):
    super(YcNetworkResource, self).__init__()
    self.name = name
    self.folder_id = folder_id

  def _Create(self):
    """Creates the Network resource."""
    cmd = util.YcCommand(self, 'vpc', 'networks', 'create')
    cmd.flags['name'] = self.name
    _, stderr, retcode = cmd.Issue()
    util.CheckYcResponseKnownFailures(stderr, retcode)

  def _Delete(self):
    """Deletes the Network resource."""
    cmd = util.YcCommand(self, 'vpc', 'networks', 'delete', self.name)
    cmd.Issue()

  def _Exists(self):
    """Returns True if the Network resource exists."""
    cmd = util.YcCommand(self, 'vpc', 'networks', 'describe', self.name)
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    return not retcode


class YcSubnetResource(resource.BaseResource):

  def __init__(self, name, network_name, zone, addr_range, folder_id):
    super(YcSubnetResource, self).__init__()
    self.name = name
    self.network_name = network_name
    self.zone = zone
    self.addr_range = addr_range
    self.folder_id = folder_id

  def _Create(self):
    cmd = util.YcCommand(self, 'vpc', 'subnets', 'create')
    cmd.flags['name'] = self.name
    cmd.flags['network-name'] = self.network_name
    cmd.flags['zone'] = self.zone
    cmd.flags['range'] = self.addr_range
    _, stderr, retcode = cmd.Issue()
    util.CheckYcResponseKnownFailures(stderr, retcode)

  def _Exists(self):
    cmd = util.YcCommand(self, 'vpc', 'subnets', 'describe',
                         self.name)
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    return not retcode

  def _Delete(self):
    cmd = util.YcCommand(self, 'vpc', 'subnets', 'delete',
                         self.name)
    cmd.Issue()



class YcNetwork(network.BaseNetwork):
  """Object representing a YC Network."""

  CLOUD = providers.YC

  def __init__(self, network_spec):
    super(YcNetwork, self).__init__(network_spec)
    self.folder_id = network_spec.folder_id
    name = FLAGS.yc_network_name or 'pkb-network-%s' % FLAGS.run_uri
    self.network_resource = YcNetworkResource(name, self.folder_id)
    subnet_zone = FLAGS.yc_subnet_zone or network_spec.zone or util.GetDefaultZone()
    self.subnet_resource = YcSubnetResource(name, name,
                                            subnet_zone,
                                            FLAGS.yc_subnet_addr,
                                            self.folder_id)

  @staticmethod
  def _GetNetworkSpecFromVm(vm):
    """Returns a BaseNetworkSpec created from VM attributes."""
    return YcNetworkSpec(folder_id=vm.folder_id, zone=vm.zone)

  @classmethod
  def _GetKeyFromNetworkSpec(cls, spec):
    """Returns a key used to register Network instances."""
    return (cls.CLOUD, spec.folder_id)

  def Create(self):
    """Creates the actual network."""
    if not FLAGS.yc_network_name:
      self.network_resource.Create()
      if self.subnet_resource:
        self.subnet_resource.Create()

  def Delete(self):
    """Deletes the actual network."""
    if not FLAGS.yc_network_name:
      if self.subnet_resource:
        self.subnet_resource.Delete()
      self.network_resource.Delete()
