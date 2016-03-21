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

import logging
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
    _lock = threading.Lock()

    def __init__(self):
        self.sec_group_rules_set = set()
        self.__nclient = utils.NovaClient()

        with self._lock:
            if not (self.__nclient.security_groups.findall(
                    name='perfkit_sc_group')):
                self.sec_group = self.__nclient.security_groups.create(
                    'perfkit_sc_group',
                    'Firewall configuration for Perfkit Benchmarker'
                )
            else:
                self.sec_group = self.__nclient.security_groups.findall(
                    name='perfkit_sc_group')[0]

    def AllowICMP(self, vm, icmp_type=-1, icmp_code=-1):
        """Creates a Security Group Rule on the Firewall to allow/disallow
        ICMP traffic.

        Args:
          vm: The BaseVirtualMachine object to allow ICMP traffic to.
          icmp_type: ICMP type to allow. If none given then allows all types.
          icmp_code: ICMP code to allow. If none given then allows all codes.
        """
        if vm.is_static:
            return

        from novaclient.exceptions import BadRequest

        sec_group_rule = ('icmp', icmp_type, icmp_code, self.sec_group.id)
        if sec_group_rule in self.sec_group_rules_set:
            return

        with self._lock:
            if sec_group_rule in self.sec_group_rules_set:
                return

            try:
                self.__nclient.security_group_rules.create(self.sec_group.id,
                                                           ip_protocol='icmp',
                                                           from_port=icmp_type,
                                                           to_port=icmp_code)
            except BadRequest:
                logging.debug('Rule icmp:%d-%d already exists' % (icmp_type,
                                                                  icmp_code))
            self.sec_group_rules_set.add(sec_group_rule)

    def AllowPort(self, vm, port, to_port=None):
        """Creates a Security Group Rule on the Firewall to allow for both TCP
        and UDP network traffic on given port, or port range.

        Args:
          vm: The BaseVirtualMachine object to open the port for.
          port: The local port to open.
          to_port: The last port to open in range of ports to open. If None,
              then only the single 'port' is open.
        """
        if vm.is_static:
            return

        from novaclient.exceptions import BadRequest

        if to_port is None:
            to_port = port

        sec_group_rule = (port, to_port, self.sec_group.id)
        if sec_group_rule in self.sec_group_rules_set:
            return

        with self._lock:
            if sec_group_rule in self.sec_group_rules_set:
                return
            for prot in ('tcp', 'udp',):
                try:
                    self.__nclient.security_group_rules.create(
                        self.sec_group.id, ip_protocol=prot,
                        from_port=port, to_port=to_port)
                except BadRequest:
                    logging.debug('Rule %s:%d-%d already exists',
                                  prot, port, to_port)

            self.sec_group_rules_set.add(sec_group_rule)

    def DisallowAllPorts(self):
        pass


class OpenStackPublicNetwork(object):

    def __init__(self, pool):
        self.__nclient = utils.NovaClient()
        # TODO(meteorfox) Is this lock needed? Callers to these methods seem
        # to be using a lock already. Should we make callers use a lock, or
        # should this object handle the lock?
        self.__floating_ip_lock = threading.Lock()
        self.ip_pool_name = pool

    def allocate(self):
        with self.__floating_ip_lock:
            return self.__nclient.floating_ips.create(pool=self.ip_pool_name)

    def release(self, floating_ip):
        f_id = floating_ip.id
        if self.__nclient.floating_ips.get(f_id):
            with self.__floating_ip_lock:
                if self.__nclient.floating_ips.get(f_id):
                    self.__nclient.floating_ips.delete(floating_ip)
                    while self.__nclient.floating_ips.findall(id=f_id):
                        time.sleep(1)

    def get_or_create(self):
        with self.__floating_ip_lock:
            floating_ips = self.__nclient.floating_ips.findall(
                instance_id=None,
                pool=self.ip_pool_name)
        if floating_ips:
            return floating_ips[0]
        else:
            return self.allocate()

    def is_attached(self, floating_ip):
        return self.__nclient.floating_ips.get(
            floating_ip.id
        ).fixed_ip is not None
