# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.oci.oci_network."""

# pylint: disable=protected-access

import json
import unittest

from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker import network
from perfkitbenchmarker.providers.oci import oci_network
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_REGION = 'us-ashburn-1'
_VCN = json.dumps({
    'data': {
        'id': 'ocid1.vcn.test',
        'cidr-block': '10.0.0.0/16',
        'default-route-table-id': 'ocid1.routetable.test',
        'default-security-list-id': 'ocid1.securitylist.test',
        'lifecycle-state': 'AVAILABLE',
    }
})
# The default security list rules as printed by the oci CLI.
_DEFAULT_SECURITY_LIST = json.dumps({
    'data': {
        'ingress-security-rules': [{
            'description': None,
            'icmp-options': None,
            'is-stateless': False,
            'protocol': '6',
            'source': '0.0.0.0/0',
            'source-type': 'CIDR_BLOCK',
            'tcp-options': {
                'destination-port-range': {'max': 22, 'min': 22},
                'source-port-range': None,
            },
            'udp-options': None,
        }]
    }
})
_AVAILABLE = ('{"data": {"lifecycle-state": "AVAILABLE"}}', '', 0)
_SSH_RULE = {
    'isStateless': False,
    'protocol': '6',
    'source': '0.0.0.0/0',
    'sourceType': 'CIDR_BLOCK',
    'tcpOptions': {'destinationPortRange': {'max': 22, 'min': 22}},
}


def _GetArg(cmd, name):
  return cmd[cmd.index(name) + 1]


class OciNetworkTestCase(pkb_common_test_case.PkbCommonTestCase):
  """Base class for OCI network tests."""

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = 'abc123'

  def _create_network(self, zone=_REGION, cidr=None):
    return oci_network.OciNetwork(network.BaseNetworkSpec(zone, cidr))

  def _get_commands(self, mock_cmd, substring):
    return [
        call[0][0]
        for call in mock_cmd.func_to_mock.call_args_list
        if substring in ' '.join(call[0][0])
    ]


class MakeIngressRuleTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for building security list ingress rules."""

  def test_tcp_port_range(self):
    """Tests tcp port range."""
    self.assertEqual(
        {
            'source': '10.0.0.0/8',
            'sourceType': 'CIDR_BLOCK',
            'protocol': oci_network.TCP,
            'isStateless': False,
            'tcpOptions': {'destinationPortRange': {'min': 80, 'max': 90}},
        },
        oci_network.MakeIngressRule(oci_network.TCP, '10.0.0.0/8', 80, 90),
    )

  def test_udp_single_port(self):
    """Tests udp single port."""
    rule = oci_network.MakeIngressRule(oci_network.UDP, start_port=5001)
    self.assertEqual(
        {'destinationPortRange': {'min': 5001, 'max': 5001}},
        rule['udpOptions'],
    )
    self.assertEqual('0.0.0.0/0', rule['source'])

  def test_icmp_has_no_port_options(self):
    """Tests icmp has no port options."""
    rule = oci_network.MakeIngressRule(oci_network.ICMP, start_port=22)
    self.assertNotIn('tcpOptions', rule)
    self.assertNotIn('udpOptions', rule)

  def test_cli_rules_compare_equal_to_built_rules(self):
    """Tests cli rules compare equal to built rules."""
    cli_rules = json.loads(_DEFAULT_SECURITY_LIST)['data'][
        'ingress-security-rules'
    ]
    self.assertEqual(
        [oci_network.MakeIngressRule(oci_network.TCP, start_port=22)],
        oci_network._ToCamelCase(cli_rules),
    )


class OciNetworkTest(OciNetworkTestCase):
  """Tests for creating and deleting OCI networks."""

  def test_network_key_is_shared_across_availability_domains(self):
    """Tests network key is shared across availability domains."""
    self.assertEqual(
        oci_network.OciNetwork._GetKeyFromNetworkSpec(
            network.BaseNetworkSpec('us-ashburn-1-ad-1')
        ),
        oci_network.OciNetwork._GetKeyFromNetworkSpec(
            network.BaseNetworkSpec('us-ashburn-1-ad-2')
        ),
    )

  def test_init(self):
    """Tests init."""
    net = self._create_network(zone='us-ashburn-1-ad-2')
    self.assertEqual(_REGION, net.region)
    self.assertEqual('pkb-abc123', net.name)
    self.assertFalse(net.is_existing_network)
    self.assertEqual(oci_network.DEFAULT_SUBNET_CIDR, net.subnet.cidr)

  def test_init_with_cidr(self):
    """Tests init with cidr."""
    net = self._create_network(cidr='10.0.5.0/24')
    self.assertEqual('10.0.5.0/24', net.subnet.cidr)

  def test_create(self):
    """Tests create."""
    mock_cmd = self.MockIssueCommand({
        'vcn create': [(_VCN, '', 0)],
        'internet-gateway create': [
            ('{"data": {"id": "ocid1.internetgateway.test"}}', '', 0)
        ],
        'subnet create': [('{"data": {"id": "ocid1.subnet.test"}}', '', 0)],
        'security-list get': [(_DEFAULT_SECURITY_LIST, '', 0)],
        ' get ': [_AVAILABLE],
    })
    net = self._create_network()

    net.Create()

    self.assertEqual('ocid1.vcn.test', net.vcn.id)
    self.assertEqual('ocid1.subnet.test', net.subnet_id)
    self.assertEqual('ocid1.internetgateway.test', net.internet_gateway.id)

    vcn_cmd = self._get_commands(mock_cmd, 'vcn create')[0]
    self.assertEqual('pkb-abc123', _GetArg(vcn_cmd, '--display-name'))
    self.assertEqual('["10.0.0.0/16"]', _GetArg(vcn_cmd, '--cidr-blocks'))
    self.assertEqual('vcnabc123', _GetArg(vcn_cmd, '--dns-label'))
    self.assertEqual(_REGION, _GetArg(vcn_cmd, '--region'))

    route_cmd = self._get_commands(mock_cmd, 'route-table update')[0]
    self.assertEqual('ocid1.routetable.test', _GetArg(route_cmd, '--rt-id'))
    self.assertEqual(
        [{
            'destination': '0.0.0.0/0',
            'destinationType': 'CIDR_BLOCK',
            'networkEntityId': 'ocid1.internetgateway.test',
        }],
        json.loads(_GetArg(route_cmd, '--route-rules')),
    )

    subnet_cmd = self._get_commands(mock_cmd, 'subnet create')[0]
    self.assertEqual('ocid1.vcn.test', _GetArg(subnet_cmd, '--vcn-id'))
    self.assertEqual('10.0.0.0/24', _GetArg(subnet_cmd, '--cidr-block'))

    # The VCN's own CIDR is opened for all protocols, keeping existing rules.
    security_cmd = self._get_commands(mock_cmd, 'security-list update')[0]
    self.assertEqual(
        [
            _SSH_RULE,
            oci_network.MakeIngressRule(
                oci_network.ALL_PROTOCOLS, '10.0.0.0/16'
            ),
        ],
        json.loads(_GetArg(security_cmd, '--ingress-security-rules')),
    )

  def test_create_ordering(self):
    """Tests create ordering."""
    mock_cmd = self.MockIssueCommand({
        'vcn create': [(_VCN, '', 0)],
        'internet-gateway create': [('{"data": {"id": "ig"}}', '', 0)],
        'subnet create': [('{"data": {"id": "subnet"}}', '', 0)],
        'security-list get': [(_DEFAULT_SECURITY_LIST, '', 0)],
        ' get ': [_AVAILABLE],
    })
    self._create_network().Create()
    commands = mock_cmd.all_commands
    self.assertLess(
        commands.index('vcn create'), commands.index('internet-gateway create')
    )
    self.assertLess(
        commands.index('internet-gateway create'),
        commands.index('route-table update'),
    )
    self.assertLess(
        commands.index('route-table update'), commands.index('subnet create')
    )

  @flagsaver.flagsaver(oci_network_name='my-vcn')
  def test_create_with_existing_network(self):
    """Tests create with existing network."""
    mock_cmd = self.MockIssueCommand({
        'vcn list': [(
            json.dumps({
                'data': [
                    {
                        'id': 'ocid1.vcn.deleted',
                        'cidr-block': '10.1.0.0/16',
                        'default-route-table-id': 'rt0',
                        'default-security-list-id': 'sl0',
                        'lifecycle-state': 'TERMINATED',
                    },
                    {
                        'id': 'ocid1.vcn.existing',
                        'cidr-block': '10.2.0.0/16',
                        'default-route-table-id': 'rt',
                        'default-security-list-id': 'sl',
                        'lifecycle-state': 'AVAILABLE',
                    },
                ]
            }),
            '',
            0,
        )],
        'subnet list': [(
            json.dumps({
                'data': [{
                    'id': 'ocid1.subnet.existing',
                    'cidr-block': '10.2.1.0/24',
                    'lifecycle-state': 'AVAILABLE',
                }]
            }),
            '',
            0,
        )],
    })
    net = self._create_network()
    self.assertTrue(net.is_existing_network)
    self.assertEqual('my-vcn', net.name)

    net.Create()

    self.assertEqual('ocid1.vcn.existing', net.vcn.id)
    self.assertEqual('10.2.0.0/16', net.vcn.cidr)
    self.assertEqual('ocid1.subnet.existing', net.subnet_id)
    self.assertNotIn('create', mock_cmd.all_commands)
    self.assertNotIn('update', mock_cmd.all_commands)

  @flagsaver.flagsaver(oci_network_name='missing-vcn')
  def test_create_with_missing_existing_network(self):
    """Tests create with missing existing network."""
    self.MockIssueCommand({'vcn list': [('{"data": []}', '', 0)]})
    with self.assertRaises(errors.Config.InvalidValue):
      self._create_network().Create()

  @flagsaver.flagsaver(oci_network_name='my-vcn')
  def test_create_with_existing_network_without_subnets(self):
    """Tests create with existing network without subnets."""
    self.MockIssueCommand({
        'vcn list': [(
            json.dumps({
                'data': [{
                    'id': 'ocid1.vcn.existing',
                    'cidr-block': '10.2.0.0/16',
                    'default-route-table-id': 'rt',
                    'default-security-list-id': 'sl',
                    'lifecycle-state': 'AVAILABLE',
                }]
            }),
            '',
            0,
        )],
        'subnet list': [('{"data": []}', '', 0)],
    })
    with self.assertRaises(errors.Config.InvalidValue):
      self._create_network().Create()

  def test_delete(self):
    """Tests delete."""
    mock_cmd = self.MockIssueCommand({
        ' get ': [('{"data": {"lifecycle-state": "TERMINATED"}}', '', 0)],
    })
    net = self._create_network()
    net.vcn.id = 'ocid1.vcn.test'
    net.vcn.default_route_table_id = 'ocid1.routetable.test'
    net.internet_gateway.id = 'ocid1.internetgateway.test'
    net.subnet.id = 'ocid1.subnet.test'
    for resource in (net.vcn, net.internet_gateway, net.subnet):
      resource.created = True

    net.Delete()

    commands = mock_cmd.all_commands
    self.assertLess(
        commands.index('subnet delete'), commands.index('route-table update')
    )
    self.assertLess(
        commands.index('route-table update'),
        commands.index('internet-gateway delete'),
    )
    self.assertLess(
        commands.index('internet-gateway delete'),
        commands.index('vcn delete'),
    )
    route_cmd = self._get_commands(mock_cmd, 'route-table update')[0]
    self.assertEqual('[]', _GetArg(route_cmd, '--route-rules'))

  @flagsaver.flagsaver(oci_network_name='my-vcn')
  def test_delete_existing_network_is_noop(self):
    """Tests delete existing network is noop."""
    mock_cmd = self.MockIssueCommand({})
    self._create_network().Delete()
    mock_cmd.func_to_mock.assert_not_called()

  def test_add_ingress_rules_skips_existing_rules(self):
    """Tests add ingress rules skips existing rules."""
    mock_cmd = self.MockIssueCommand(
        {'security-list get': [(_DEFAULT_SECURITY_LIST, '', 0)]}
    )
    net = self._create_network()
    net.vcn.default_security_list_id = 'ocid1.securitylist.test'

    net.AddIngressRules(
        [oci_network.MakeIngressRule(oci_network.TCP, start_port=22)]
    )

    self.assertNotIn('security-list update', mock_cmd.all_commands)


class OciFirewallTest(OciNetworkTestCase):
  """Tests for opening ports with OciFirewall."""

  def setUp(self):
    super().setUp()
    self.firewall = oci_network.OciFirewall()
    self.vm = mock.Mock(is_static=False)
    self.vm.network.is_existing_network = False
    self.vm.network.region = _REGION

  def test_allow_port_opens_tcp_and_udp(self):
    """Tests allow port opens tcp and udp."""
    self.firewall.AllowPort(self.vm, 5000, 5010, ['10.0.0.0/8'])
    self.vm.network.AddIngressRules.assert_called_once_with([
        oci_network.MakeIngressRule(oci_network.TCP, '10.0.0.0/8', 5000, 5010),
        oci_network.MakeIngressRule(oci_network.UDP, '10.0.0.0/8', 5000, 5010),
    ])

  def test_allow_port_defaults_to_all_sources(self):
    """Tests allow port defaults to all sources."""
    self.firewall.AllowPort(self.vm, 22)
    self.vm.network.AddIngressRules.assert_called_once_with([
        oci_network.MakeIngressRule(oci_network.TCP, '0.0.0.0/0', 22, 22),
        oci_network.MakeIngressRule(oci_network.UDP, '0.0.0.0/0', 22, 22),
    ])

  def test_allow_port_is_only_applied_once(self):
    """Tests allow port is only applied once."""
    self.firewall.AllowPort(self.vm, 22)
    self.firewall.AllowPort(self.vm, 22)
    self.vm.network.AddIngressRules.assert_called_once()

  def test_allow_icmp(self):
    """Tests allow icmp."""
    self.firewall.AllowIcmp(self.vm)
    self.firewall.AllowIcmp(self.vm)
    self.vm.network.AddIngressRules.assert_called_once_with(
        [oci_network.MakeIngressRule(oci_network.ICMP)]
    )

  def test_existing_network_is_not_modified(self):
    """Tests existing network is not modified."""
    self.vm.network.is_existing_network = True
    self.firewall.AllowPort(self.vm, 22)
    self.firewall.AllowIcmp(self.vm)
    self.vm.network.AddIngressRules.assert_not_called()

  def test_static_vm_is_not_modified(self):
    """Tests static vm is not modified."""
    self.vm.is_static = True
    self.firewall.AllowPort(self.vm, 22)
    self.vm.network.AddIngressRules.assert_not_called()


if __name__ == '__main__':
  unittest.main()
