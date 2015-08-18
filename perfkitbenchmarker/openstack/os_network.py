# Copyright 2015 Google Inc. All rights reserved.
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
from perfkitbenchmarker.openstack import utils

FLAGS = flags.FLAGS

flags.DEFINE_string('openstack_public_network', None,
                    'Name of OpenStack public network')

flags.DEFINE_string('openstack_private_network', 'private',
                    'Name of OpenStack private network')

MAX_PORT = 65535


class OpenStackFirewall(network.BaseFirewall):
    """
    An object representing OpenStack Firewall based on Secure Groups.
    """

    def __init__(self, project):
        super(OpenStackFirewall, self).__init__(project)
        self.project = project
        self.__nclient = utils.NovaClient()
        self.secgroup_id = self.__get_secgroup_id()


        self.AllowPort(None, -1, protocol='icmp')
        self.AllowPort(None, 1, MAX_PORT)


    def __get_secgroup_id(self):
        # create group if not exists
        if not (self.__nclient.security_groups.findall(
                name='perfkit_sc_group')):
            sec_group = self.__nclient.security_groups.create(
                'perfkit_sc_group',
                'Firewall configuration for Perfkit Benchmarker'
            )
        else:
            sec_group = self.__nclient.security_groups.findall(
                name='perfkit_sc_group')[0]
        return sec_group.id

    def AllowPort(self, vm, from_port, to_port=None, protocol='tcp'):
        if to_port is None:
            to_port = from_port

        try:
            self.__nclient.security_group_rules.create(self.secgroup_id,
                                                       ip_protocol=protocol,
                                                       from_port=from_port,
                                                       to_port=to_port)
        except Exception:
            pass

    def DisallowAllPorts(self):
        pass
