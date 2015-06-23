from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker.openstack import utils

FLAGS = flags.FLAGS

flags.DEFINE_string('os_public_network', None,
                    'Name of OpenStack public network')

flags.DEFINE_string('os_private_network', 'private',
                    'Name of OpenStack private network')


class OpenStackFirewall(network.BaseFirewall):
    """
    An object representing OpenStack Firewall based on Secure Groups.
    """

    def __init__(self, project):
        super(OpenStackFirewall, self).__init__(project)
        self.project = project
        self.__nclient = utils.NovaClient(FLAGS.os_auth_url,
                                          FLAGS.os_tenant,
                                          FLAGS.os_username,
                                          FLAGS.os_passwd
                                          )

        if not (self.__nclient.security_groups.findall(name='perfkit_sc_group')):
            self.sec_group = self.__nclient.security_groups.create(
                'perfkit_sc_group',
                'Firewall configuration for Perfkit Benchmarker'
            )
        else:
            self.sec_group = self.__nclient.security_groups.findall(name='perfkit_sc_group')[0]

        self.AllowPort(None, -1, 'icmp')
        self.AllowPort(None, 22)
        self.AllowPort(None, 80)
        self.AllowPort(None, 3000)

    def AllowPort(self, vm, port, protocol='tcp'):
        try:
            self.__nclient.security_group_rules.create(self.sec_group.id,
                                                       ip_protocol=protocol,
                                                       from_port=port,
                                                       to_port=port)
        except Exception:
            pass

    def DisallowAllPorts(self):
        pass


class OpenStackNetwork(network.BaseNetwork):
    pass
