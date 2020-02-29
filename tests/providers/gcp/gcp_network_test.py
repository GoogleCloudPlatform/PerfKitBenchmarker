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
"""Tests for perfkitbenchmarker.providers.gcp.gce_network."""

import contextlib

import mock

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_benchmarks
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import util
from tests import pkb_common_test_case
from six.moves import builtins

FLAGS = flags.FLAGS

_PROJECT = 'project'
_CLOUD = 'GCP'
_BENCHMARK_NAME = 'iperf'
_URI = 'uri45678'
_COMPONENT = 'test_component'

_CFG_DEFAULT_DEFAULT = """
iperf:
  vm_groups:
    vm_1:
      cloud: GCP
      vm_spec:
        GCP:
            zone: us-west1-b
            machine_type: n1-standard-4
    vm_2:
      cloud: GCP
      vm_spec:
        GCP:
            zone: us-central1-c
            machine_type: n1-standard-4
"""

_CFG_MULTI_MULTI = """
iperf:
  vm_groups:
    vm_1:
      cloud: GCP
      cidr: 10.0.1.0/24
      vm_spec:
        GCP:
            zone: us-west1-b
            machine_type: n1-standard-4
    vm_2:
      cloud: GCP
      cidr: 192.168.1.0/24
      vm_spec:
        GCP:
            zone: us-central1-c
            machine_type: n1-standard-4
"""

_CFG_DEFAULT_MULTI = """
iperf:
  vm_groups:
    vm_1:
      cloud: GCP
      vm_spec:
        GCP:
            zone: us-west1-b
            machine_type: n1-standard-4
    vm_2:
      cloud: GCP
      cidr: 192.168.1.0/24
      vm_spec:
        GCP:
            zone: us-central1-c
            machine_type: n1-standard-4
"""

_CFG_SAME_ZONE_AND_CIDR = """
iperf:
  vm_groups:
    vm_1:
      cloud: GCP
      cidr: 10.0.1.0/24
      vm_spec:
        GCP:
            zone: us-west1-b
            machine_type: n1-standard-4
    vm_2:
      cloud: GCP
      cidr: 10.0.1.0/24
      vm_spec:
        GCP:
            zone: us-west1-b
            machine_type: n1-standard-4
"""

_CFG_SAME_ZONE_DIFF_CIDR = """
iperf:
  vm_groups:
    vm_1:
      cloud: GCP
      cidr: 10.0.1.0/24
      vm_spec:
        GCP:
            zone: us-west1-b
            machine_type: n1-standard-4
    vm_2:
      cloud: GCP
      cidr: 10.0.2.0/24
      vm_spec:
        GCP:
            zone: us-west1-b
            machine_type: n1-standard-4
"""

_REGEX_GCE_NET_NAMES = r'(?:[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?)'
_REGEX_GCE_FW_NAMES = r'(?:[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?)'


class BaseGceNetworkTest(pkb_common_test_case.PkbCommonTestCase):

  def _CreateBenchmarkSpecFromYaml(self, yaml_string,
                                   benchmark_name=_BENCHMARK_NAME):
    config = configs.LoadConfig(yaml_string, {}, benchmark_name)
    return self._CreateBenchmarkSpecFromConfigDict(config, benchmark_name)

  def _CreateBenchmarkSpecFromConfigDict(self, config_dict, benchmark_name):
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        benchmark_name,
        flag_values=FLAGS,
        **config_dict)
    benchmark_module = next((b for b in linux_benchmarks.BENCHMARKS
                             if b.BENCHMARK_NAME == benchmark_name))
    return benchmark_spec.BenchmarkSpec(benchmark_module, config_spec, _URI)


class TestGceNetworkConfig(BaseGceNetworkTest):

  def testLoadDefaultConfig(self):
    spec = self._CreateBenchmarkSpecFromYaml(_CFG_DEFAULT_DEFAULT)
    spec.ConstructVirtualMachines()

    self.assertDictContainsSubset({'cidr': None}, spec.custom_subnets['vm_1'])
    self.assertDictContainsSubset({'cidr': None}, spec.custom_subnets['vm_2'])
    self.assertLen(spec.networks, 1)
    for k in spec.networks.keys():
      self.assertItemsEqual(['10.0.0.0/8'], spec.networks[k].all_nets)

  def testLoadDefaultConfigWithFlags(self):
    FLAGS.gce_subnet_region = 'us-north1-b'
    FLAGS.gce_subnet_addr = '1.2.3.4/33'

    spec = self._CreateBenchmarkSpecFromYaml(_CFG_DEFAULT_DEFAULT)
    spec.ConstructVirtualMachines()

    self.assertDictContainsSubset({'cidr': None}, spec.custom_subnets['vm_1'])
    self.assertDictContainsSubset({'cidr': None}, spec.custom_subnets['vm_2'])
    self.assertLen(spec.networks, 1)
    for k in spec.networks.keys():
      self.assertItemsEqual(['1.2.3.4/33'], spec.networks[k].all_nets)

  def testLoadCustomConfig(self):
    spec = self._CreateBenchmarkSpecFromYaml(_CFG_MULTI_MULTI)
    spec.ConstructVirtualMachines()

    self.assertDictContainsSubset({'cidr': '10.0.1.0/24'},
                                  spec.custom_subnets['vm_1'])
    self.assertDictContainsSubset({'cidr': '192.168.1.0/24'},
                                  spec.custom_subnets['vm_2'])
    self.assertLen(spec.networks, 2)
    for k in spec.networks.keys():
      self.assertItemsEqual(['192.168.1.0/24', '10.0.1.0/24'],
                            spec.networks[k].all_nets)

  def testLoadCustomConfigWithFlags(self):
    FLAGS.gce_subnet_region = 'us-north1-b'
    FLAGS.gce_subnet_addr = '1.2.3.4/33'

    spec = self._CreateBenchmarkSpecFromYaml(_CFG_MULTI_MULTI)
    spec.ConstructVirtualMachines()

    self.assertDictContainsSubset({'cidr': '10.0.1.0/24'},
                                  spec.custom_subnets['vm_1'])
    self.assertDictContainsSubset({'cidr': '192.168.1.0/24'},
                                  spec.custom_subnets['vm_2'])
    self.assertLen(spec.networks, 2)
    for k in spec.networks.keys():
      self.assertItemsEqual(['192.168.1.0/24', '10.0.1.0/24'],
                            spec.networks[k].all_nets)

  def testLoadMixedConfig(self):
    spec = self._CreateBenchmarkSpecFromYaml(_CFG_DEFAULT_MULTI)
    spec.ConstructVirtualMachines()

    self.assertDictContainsSubset({'cidr': None}, spec.custom_subnets['vm_1'])
    self.assertDictContainsSubset({'cidr': '192.168.1.0/24'},
                                  spec.custom_subnets['vm_2'])
    self.assertLen(spec.networks, 2)
    for k in spec.networks.keys():
      self.assertItemsEqual(['10.0.0.0/8', '192.168.1.0/24'],
                            spec.networks[k].all_nets)

  def testLoadMixedConfigWithFlags(self):
    FLAGS.gce_subnet_region = 'us-north1-b'
    FLAGS.gce_subnet_addr = '1.2.3.4/33'

    spec = self._CreateBenchmarkSpecFromYaml(_CFG_DEFAULT_MULTI)
    spec.ConstructVirtualMachines()

    self.assertDictContainsSubset({'cidr': None}, spec.custom_subnets['vm_1'])
    self.assertDictContainsSubset({'cidr': '192.168.1.0/24'},
                                  spec.custom_subnets['vm_2'])
    self.assertLen(spec.networks, 2)
    for k in spec.networks.keys():
      self.assertItemsEqual(['1.2.3.4/33', '192.168.1.0/24'],
                            spec.networks[k].all_nets)

  def testLoadSameZoneCidrConfig(self):
    spec = self._CreateBenchmarkSpecFromYaml(_CFG_SAME_ZONE_AND_CIDR)
    spec.ConstructVirtualMachines()

    self.assertDictContainsSubset({'cidr': '10.0.1.0/24'},
                                  spec.custom_subnets['vm_1'])
    self.assertDictContainsSubset({'cidr': '10.0.1.0/24'},
                                  spec.custom_subnets['vm_2'])
    self.assertLen(spec.networks, 1)
    for k in spec.networks.keys():
      self.assertItemsEqual(['10.0.1.0/24'], spec.networks[k].all_nets)

  def testLoadSameZoneDiffCidrConfig(self):
    spec = self._CreateBenchmarkSpecFromYaml(_CFG_SAME_ZONE_DIFF_CIDR)
    spec.ConstructVirtualMachines()

    self.assertDictContainsSubset({'cidr': '10.0.1.0/24'},
                                  spec.custom_subnets['vm_1'])
    self.assertDictContainsSubset({'cidr': '10.0.2.0/24'},
                                  spec.custom_subnets['vm_2'])
    self.assertLen(spec.networks, 2)
    for k in spec.networks.keys():
      self.assertItemsEqual(['10.0.1.0/24', '10.0.2.0/24'],
                            spec.networks[k].all_nets)


class TestGceNetworkNames(BaseGceNetworkTest):

  def setUp(self):
    super(TestGceNetworkNames, self).setUp()
    # need a benchmarkspec in the context to run
    FLAGS.run_uri = _URI
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        'cluster_boot', flag_values=FLAGS)
    benchmark_spec.BenchmarkSpec(mock.Mock(), config_spec, 'uid')

  ########
  # Network Names
  ########
  def testGetDefaultNetworkName(self):
    project = _PROJECT
    zone = 'us-north1-b'
    cidr = None
    # long_cidr = '123.567.901/13'  # @TODO net_utils for address sanity checks
    vm = mock.Mock(zone=zone, project=project, cidr=cidr)
    net = gce_network.GceNetwork.GetNetwork(vm)
    net_name = net._MakeGceNetworkName()

    net_type = 'default'
    cidr_string = None
    uri = _URI
    expected_netname = '-'.join(
        i for i in ('pkb-network', net_type, cidr_string, uri) if
        i and i not in 'default')

    self.assertEqual(expected_netname,
                     net_name)  # pkb-network-uri45678 (default)
    self.assertRegexpMatches(net_name, _REGEX_GCE_NET_NAMES)

  def testGetSingleNetworkName(self):
    FLAGS.gce_subnet_region = 'us-south1-c'
    FLAGS.gce_subnet_addr = '2.2.3.4/33'

    project = _PROJECT
    zone = 'us-north1-b'
    cidr = None
    vm = mock.Mock(zone=zone, project=project, cidr=cidr)
    net = gce_network.GceNetwork.GetNetwork(vm)
    net_name = net._MakeGceNetworkName()

    net_type = 'single'
    cidr_string = '2-2-3-4-33'
    uri = _URI
    expected_netname = '-'.join(
        i for i in ('pkb-network', net_type, cidr_string, uri) if
        i and i not in 'default')

    self.assertEqual(
        expected_netname,
        net_name)  # pkb-network-single-2-2-3-4-33-uri45678 (single)
    self.assertRegexpMatches(net_name, _REGEX_GCE_NET_NAMES)

  def testGetMultiNetworkName(self):
    project = _PROJECT
    zone = 'us-north1-b'
    cidr = '1.2.3.4/56'
    vm = mock.Mock(zone=zone, project=project, cidr=cidr)
    net = gce_network.GceNetwork.GetNetwork(vm)
    net_name = net._MakeGceNetworkName()

    net_type = 'multi'
    cidr_string = '1-2-3-4-56'
    uri = _URI
    expected_netname = '-'.join(
        i for i in ('pkb-network', net_type, cidr_string, uri) if
        i and i not in 'default')

    self.assertEqual(expected_netname,
                     net_name)  # pkb-network-multi-1-2-3-4-56-uri45678 (multi)
    self.assertRegexpMatches(net_name, _REGEX_GCE_NET_NAMES)

  ########
  # FireWall Names
  ########
  def testGetDefaultFWName(self):
    project = _PROJECT
    zone = 'us-north1-b'
    cidr = None
    vm = mock.Mock(zone=zone, project=project, cidr=cidr)
    net = gce_network.GceNetwork.GetNetwork(vm)
    fw_name = net._MakeGceFWRuleName()

    net_type = 'default'
    src_cidr_string = 'internal'
    dst_cidr_string = '10-0-0-0-8'
    src_port = None
    dst_port = None
    uri = _URI
    expected_name = '-'.join(
        i for i in (
            net_type, src_cidr_string, dst_cidr_string, src_port,
            dst_port, uri) if i)

    self.assertEqual(expected_name, fw_name)
    self.assertRegexpMatches(fw_name, _REGEX_GCE_FW_NAMES)

  def testGetSingleFWName(self):
    FLAGS.gce_subnet_region = 'us-south1-c'
    FLAGS.gce_subnet_addr = '2.2.3.4/33'

    project = _PROJECT
    zone = 'us-north1-b'
    cidr = None
    lo_port = None
    hi_port = None
    vm = mock.Mock(zone=zone, project=project, cidr=cidr)
    net = gce_network.GceNetwork.GetNetwork(vm)
    fw_name = net._MakeGceFWRuleName(
        net_type=None, src_cidr=None, dst_cidr=None, port_range_lo=lo_port,
        port_range_hi=hi_port, uri=None)

    net_type = 'single'
    src_cidr_string = 'internal'
    dst_cidr_string = '2-2-3-4-33'
    src_port = None
    dst_port = None
    uri = _URI
    expected_name = '-'.join(
        i for i in (net_type, src_cidr_string, dst_cidr_string, src_port,
                    dst_port, uri)
        if i)

    self.assertEqual(expected_name,
                     fw_name)  # single-internal-2-2-3-4-33-uri45678
    self.assertRegexpMatches(fw_name, _REGEX_GCE_FW_NAMES)

  def testGetMultiFWNameWithPorts(self):
    project = _PROJECT
    zone = 'us-north1-b'
    cidr = '1.2.3.4/56'
    # cidr = None
    vm = mock.Mock(zone=zone, project=project, cidr=cidr)
    net = gce_network.GceNetwork.GetNetwork(vm)

    dst_cidr = None
    lo_port = 49152
    hi_port = 65535
    fw_name = net._MakeGceFWRuleName(
        net_type=None, src_cidr=None, dst_cidr=dst_cidr,
        port_range_lo=lo_port,
        port_range_hi=hi_port, uri=None)

    prefix = None
    net_type = 'multi'
    src_cidr_string = 'internal'
    dst_cidr_string = '1-2-3-4-56'
    src_port = '49152'
    dst_port = '65535'
    uri = _URI
    expected_name = '-'.join(
        i for i in (prefix, net_type, src_cidr_string, dst_cidr_string,
                    src_port, dst_port, uri) if i)

    self.assertEqual(expected_name,
                     fw_name)  # multi-internal-1-2-3-4-56-49152-65535-uri45678
    self.assertRegexpMatches(fw_name, _REGEX_GCE_FW_NAMES)

  def testGetMultiFWNameWithPortsDst(self):
    project = _PROJECT
    zone = 'us-north1-b'
    cidr = '1.2.3.4/56'
    vm = mock.Mock(zone=zone, project=project, cidr=cidr)
    net = gce_network.GceNetwork.GetNetwork(vm)

    dst_cidr = '123.567.9.1/13'
    lo_port = 49152
    hi_port = 65535
    fw_name = net._MakeGceFWRuleName(
        net_type=None, src_cidr=None, dst_cidr=dst_cidr,
        port_range_lo=lo_port,
        port_range_hi=hi_port, uri=None)

    prefix = 'perfkit-firewall'
    net_type = 'multi'
    src_cidr_string = '1-2-3-4-56'
    dst_cidr_string = '123-567-9-1-13'
    src_port = '49152'
    dst_port = '65535'
    uri = _URI
    expected_name = '-'.join(
        i for i in (prefix, net_type, src_cidr_string, dst_cidr_string,
                    src_port, dst_port, uri) if i)

    # perfkit-firewall-multi-1-2-3-4-56-123-567-901-13-49152-65535-uri45678
    self.assertEqual(expected_name, fw_name)
    self.assertRegexpMatches(fw_name, _REGEX_GCE_FW_NAMES)


#  found in tests/gce_virtual_machine_test.py
@contextlib.contextmanager
def PatchCriticalObjects(retvals=None):
  """A context manager that patches a few critical objects with mocks."""

  def ReturnVal(*unused_arg, **unused_kwargs):
    del unused_arg
    del unused_kwargs
    return ('', '', 0) if retvals is None else retvals.pop(0)

  with mock.patch(
      vm_util.__name__ + '.IssueCommand',
      side_effect=ReturnVal) as issue_command, mock.patch(
          builtins.__name__ + '.open'), mock.patch(
              vm_util.__name__ +
              '.NamedTemporaryFile'), mock.patch(util.__name__ +
                                                 '.GetDefaultProject'):
    yield issue_command


class TestGceNetwork(BaseGceNetworkTest):

  def setUp(self):
    super(TestGceNetwork, self).setUp()
    # need a benchmarkspec in the context to run
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        'cluster_boot', flag_values=FLAGS)
    benchmark_spec.BenchmarkSpec(mock.Mock(), config_spec, 'uid')

  def testGetNetwork(self):
    project = 'myproject'
    zone = 'us-east1-a'
    vm = mock.Mock(zone=zone, project=project, cidr=None)
    net = gce_network.GceNetwork.GetNetwork(vm)
    self.assertEqual(project, net.project)
    self.assertEqual(zone, net.zone)


class GceFirewallRuleTest(pkb_common_test_case.PkbCommonTestCase):

  @mock.patch('time.sleep', side_effect=lambda _: None)
  def testGceFirewallRuleSuccessfulAfterRateLimited(self, mock_cmd):
    fake_rets = [('stdout', 'Rate Limit Exceeded', 1),
                 ('stdout', 'some warning perhaps', 0)]
    with PatchCriticalObjects(fake_rets) as issue_command:
      fr = gce_network.GceFirewallRule('name', 'project', 'allow',
                                       'network_name')
      fr._Create()
      self.assertEqual(issue_command.call_count, 2)

  @mock.patch('time.sleep', side_effect=lambda _: None)
  def testGceFirewallRuleGenericErrorAfterRateLimited(self, mock_cmd):
    fake_rets = [('stdout', 'Rate Limit Exceeded', 1),
                 ('stdout', 'Rate Limit Exceeded', 1),
                 ('stdout', 'some random firewall error', 1)]
    with PatchCriticalObjects(fake_rets) as issue_command:
      with self.assertRaises(errors.VmUtil.IssueCommandError):
        fr = gce_network.GceFirewallRule('name', 'project', 'allow',
                                         'network_name')
        fr._Create()
      self.assertEqual(issue_command.call_count, 3)

  @mock.patch('time.sleep', side_effect=lambda _: None)
  def testGceFirewallRuleAlreadyExistsAfterRateLimited(self, mock_cmd):
    fake_rets = [('stdout', 'Rate Limit Exceeded', 1),
                 ('stdout', 'Rate Limit Exceeded', 1),
                 ('stdout', 'firewall already exists', 1)]
    with PatchCriticalObjects(fake_rets) as issue_command:
      fr = gce_network.GceFirewallRule('name', 'project', 'allow',
                                       'network_name')
      fr._Create()
      self.assertEqual(issue_command.call_count, 3)

  @mock.patch('time.sleep', side_effect=lambda _: None)
  def testGceFirewallRuleGenericError(self, mock_cmd):
    fake_rets = [('stdout', 'some random firewall error', 1)]
    with PatchCriticalObjects(fake_rets) as issue_command:
      with self.assertRaises(errors.VmUtil.IssueCommandError):
        fr = gce_network.GceFirewallRule('name', 'project', 'allow',
                                         'network_name')
        fr._Create()
      self.assertEqual(issue_command.call_count, 1)
