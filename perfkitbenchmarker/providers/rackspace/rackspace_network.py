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
"""Module containing classes related to Rackspace VM networking.

The SecurityGroup class provides a way of opening VM ports. The Network class
allows VMs to communicate via internal IPs.
"""
import json
import os
import threading

from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.rackspace import util
from perfkitbenchmarker import providers

FLAGS = flags.FLAGS

SSH_PORT = 22


class RackspaceSecurityGroup(network.BaseFirewall):
    """An object representing the Rackspace Security Group."""

    CLOUD = providers.RACKSPACE

    def __init__(self):
        """Initialize Rackspace security group class."""
        self._lock = threading.Lock()
        self.firewall_names = set()
        self.sg_counter = 0

    def AllowPort(self, vm, port):
        """Opens a port on the firewall.

        Args:
          vm: The BaseVirtualMachine object to open the port for.
          port: The local port to open.
        """
        if vm.is_static or not FLAGS.use_security_group or port == SSH_PORT:
            return
        with self._lock:
            firewall_name = ('perfkit-firewall-%s-%d-%d' %
                             (FLAGS.run_uri, port, self.sg_counter))
            self.sg_counter += 1
            if firewall_name in self.firewall_names:
                return

            firewall_env = dict(os.environ.copy(),
                                **util.GetDefaultRackspaceNeutronEnv(self))

            firewall_cmd = [FLAGS.neutron_path]
            firewall_cmd.extend(['security-group-create'])
            firewall_cmd.append(firewall_name)

            vm_util.IssueRetryableCommand(firewall_cmd, env=firewall_env)

            self.firewall_names.add(firewall_name)

            for protocol in ['tcp', 'udp']:
                rule_cmd = []
                rule_cmd.extend([FLAGS.neutron_path,
                                 'security-group-rule-create',
                                 '--direction', 'ingress',
                                 '--ethertype', 'IPv4',
                                 '--protocol', protocol,
                                 '--port-range-min', str(port),
                                 '--port-range-max', str(port)])
                rule_cmd.append(firewall_name)

                vm_util.IssueRetryableCommand(rule_cmd, env=firewall_env)

            rule_cmd = []
            rule_cmd.extend([FLAGS.neutron_path,
                             'security-group-rule-create',
                             '--direction', 'ingress',
                             '--ethertype', 'IPv4',
                             '--protocol', 'tcp',
                             '--port-range-min', str(SSH_PORT),
                             '--port-range-max', str(SSH_PORT)])
            rule_cmd.append(firewall_name)

            vm_util.IssueRetryableCommand(rule_cmd, env=firewall_env)

            getport_cmd = []
            getport_cmd.extend([FLAGS.neutron_path, 'port-list',
                                '--format', 'table'])

            stdout, _ = vm_util.IssueRetryableCommand(getport_cmd,
                                                      env=firewall_env)
            attrs = stdout.split('\n')
            for attr in attrs:
                if vm.ip_address in attr or vm.ip_address6 in attr:
                    port_id = [v.strip() for v in attr.split('|') if v != ''][0]
                    if port_id != '':
                        break

            if not port_id:
              raise ValueError('Could not find port_id from response.')

            updateport_cmd = []
            updateport_cmd.extend([FLAGS.neutron_path, 'port-update'])
            for firewall in self.firewall_names:
                updateport_cmd.extend(['--security-group', firewall])
            updateport_cmd.append(port_id)

            vm_util.IssueRetryableCommand(updateport_cmd, env=firewall_env)

    def DisallowAllPorts(self):
        """Closes all ports on the firewall."""
        firewall_env = dict(os.environ.copy(),
                            **util.GetDefaultRackspaceNeutronEnv(self))

        for firewall in self.firewall_names:
            firewall_cmd = []
            firewall_cmd.extend([FLAGS.neutron_path,
                                 'security-group-show',
                                 '--format', 'value'])
            firewall_cmd.append(firewall)

            stdout, _ = vm_util.IssueRetryableCommand(firewall_cmd,
                                                      env=firewall_env)

            rules = [v for v in stdout.split('\n') if v != ''][2:-1]
            for rule in rules:
                rule_id = str(json.loads(rule)['id'])
                rule_cmd = []
                rule_cmd.extend([FLAGS.neutron_path,
                                 'security-group-rule-delete'])
                rule_cmd.append(rule_id)

                vm_util.IssueRetryableCommand(rule_cmd, env=firewall_env)

            firewall_cmd = [FLAGS.neutron_path]
            firewall_cmd.extend(['security-group-delete'])
            firewall_cmd.append(firewall)

            vm_util.IssueRetryableCommand(firewall_cmd, env=firewall_env)

            self.firewall_names.remove(firewall)
