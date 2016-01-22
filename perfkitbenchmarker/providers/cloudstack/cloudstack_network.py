# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.cloudstack import util
from perfkitbenchmarker import providers

FLAGS = flags.FLAGS


class CloudStackNetwork(network.BaseNetwork):
  """Object representing a CloudStack Network."""

  CLOUD = providers.CLOUDSTACK

  def __init__(self, spec):

    super(CloudStackNetwork, self).__init__(spec)

    self.cs = util.CsClient(FLAGS.CS_API_URL,
                            FLAGS.CS_API_KEY,
                            FLAGS.CS_API_SECRET)

    self.project_id = None
    self.network_id = None


  def _AcquireNetworkDetails(self):

    if FLAGS.project:
        project = self.cs.get_project(FLAGS.project)
        if project:
            self.project_id = project['id']

    self.zone_id = None

    zone = self.cs.get_zone(self.zone)
    if zone:
        self.zone_id = zone['id']

    assert self.zone_id, "Zone required to create a network"

    self.network_name = None

    nw_off = self.cs.get_network_offering(FLAGS.cs_network_offering,
                                          self.project_id)

    assert nw_off, "Network offering not found"

    self.network_offering_id = nw_off['id']
    self.network_name = 'perfkit-network-%s' % FLAGS.run_uri

    self.is_vpc = FLAGS.cs_use_vpc
    self.vpc_id = None

    if FLAGS.cs_use_vpc:

        assert FLAGS.cs_vpc_offering, "VPC flag should specify the VPC offering"
        vpc_off = self.cs.get_vpc_offering(FLAGS.cs_vpc_offering)
        assert vpc_off, "Use VPC specified but VPC offering not found"

        self.vpc_offering_id = vpc_off['id']
        self.vpc_name = 'perfkit-vpc-%s' % FLAGS.run_uri


  @vm_util.Retry(max_retries=3)
  def Create(self):
    """Creates the actual network."""

    gateway = None
    netmask = None

    self._AcquireNetworkDetails()

    if self.is_vpc:
        # Create a VPC first

        cidr = '10.0.0.0/16'
        vpc = self.cs.create_vpc(self.vpc_name,
                                 self.zone_id,
                                 cidr,
                                 self.vpc_offering_id,
                                 self.project_id)
        self.vpc_id = vpc['id']
        gateway = '10.0.0.1'
        netmask = '255.255.255.0'

        acl = self.cs.get_network_acl('default_allow', self.project_id)
        assert acl, "Default allow ACL not found"


    # Create the network
    network = self.cs.create_network(self.network_name,
                                     self.network_offering_id,
                                     self.zone_id,
                                     self.project_id,
                                     self.vpc_id,
                                     gateway,
                                     netmask,
                                     acl['id'])



    assert network, "No network could be created"

    self.network_id = network['id']
    self.id = self.network_id

  def Delete(self):
    """Deletes the actual network."""

    if self.network_id:
        self.cs.delete_network(self.network_id)

    if self.is_vpc and self.vpc_id:
        self.cs.delete_vpc(self.vpc_id)
