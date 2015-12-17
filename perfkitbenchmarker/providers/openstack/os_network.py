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
import threading
import time

from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker.providers.openstack import utils
from perfkitbenchmarker import providers

FLAGS = flags.FLAGS

MAX_PORT = 65535


class OpenStackFirewall(network.BaseFirewall):
    """
    An object representing OpenStack Firewall based on Secure Groups.
    """

    CLOUD = providers.OPENSTACK

    def __init__(self):
        super(OpenStackFirewall, self).__init__()
        self.__nclient = utils.NovaClient()

        if not (self.__nclient.security_groups.findall(
                name='perfkit_sc_group')):
            self.sec_group = self.__nclient.security_groups.create(
                'perfkit_sc_group',
                'Firewall configuration for Perfkit Benchmarker'
            )
        else:
            self.sec_group = self.__nclient.security_groups.findall(
                name='perfkit_sc_group')[0]

        self.AllowPort(None, -1, protocol='icmp')
        self.AllowPort(None, 1, MAX_PORT)

    def AllowPort(self, vm, from_port, to_port=None, protocol='tcp'):
        if to_port is None:
            to_port = from_port

        try:
            self.__nclient.security_group_rules.create(self.sec_group.id,
                                                       ip_protocol=protocol,
                                                       from_port=from_port,
                                                       to_port=to_port)
        except Exception:
            pass

    def DisallowAllPorts(self):
        pass


class OpenStackPublicNetwork(object):

    def __init__(self, pool):
        self.__nclient = utils.NovaClient()
        self.__floating_ip_lock = threading.Lock()
        self.ip_pool_name = pool

    def allocate(self):
        with self.__floating_ip_lock:
            return self.__nclient.floating_ips.create(pool=self.ip_pool_name)

    def release(self, floating_ip):
        f_id = floating_ip.id
        if self.__nclient.floating_ips.get(f_id).fixed_ip:
            with self.__floating_ip_lock:
                if not self.__nclient.floating_ips.get(f_id).fixed_ip:
                    self.__nclient.floating_ips.delete(floating_ip)
                    while self.__nclient.floating_ips.findall(id=f_id):
                        time.sleep(1)

    def get_or_create(self):
        with self.__floating_ip_lock:
            floating_ips = self.__nclient.floating_ips.findall(
                fixed_ip=None,
                pool=self.ip_pool_name
            )
        if floating_ips:
            return floating_ips[0]
        else:
            return self.allocate()

    def is_attached(self, floating_ip):
        return self.__nclient.floating_ips.get(
            floating_ip.id
        ).fixed_ip is not None
