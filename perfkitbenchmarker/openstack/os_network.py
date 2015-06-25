import os

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
        password = os.getenv('OS_PASSWORD')
        self.__nclient = utils.NovaClient(FLAGS.openstack_auth_url,
                                          FLAGS.openstack_tenant,
                                          FLAGS.openstack_username,
                                          password
                                          )

        if not (self.__nclient.security_groups.findall(name='perfkit_sc_group')):
            self.sec_group = self.__nclient.security_groups.create(
                'perfkit_sc_group',
                'Firewall configuration for Perfkit Benchmarker'
            )
        else:
            self.sec_group = self.__nclient.security_groups.findall(name='perfkit_sc_group')[0]

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


class OpenStackNetwork(network.BaseNetwork):
    pass
