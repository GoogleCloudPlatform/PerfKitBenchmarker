# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for vpn service"""
import pickle
import sys

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_benchmarks
from perfkitbenchmarker import providers
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.vpn_service import TunnelConfig
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

PROJECT = 'mock_project'
CLOUD = providers.GCP
BENCHMARK_NAME = 'iperf'
URI = 'uri45678'

DEFAULT_CFG = """
# VPN iperf config.
iperf:
  description: Run iperf over vpn
  flags:
    iperf_sending_thread_count: 5
    use_vpn: True
    vpn_service_gateway_count: 1
  vpn_service:
    tunnel_count: 1
    ike_version: 2
    routing_type: static
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


class BaseVPNServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(BaseVPNServiceTest, self).setUp()
    if not sys.warnoptions:  # https://bugs.python.org/issue33154
      import warnings
      warnings.simplefilter("ignore", ResourceWarning)

  def _CreateBenchmarkSpecFromYaml(self, yaml_string,
                                   benchmark_name=BENCHMARK_NAME):
    config = configs.LoadConfig(yaml_string, {}, benchmark_name)
    return self._CreateBenchmarkSpecFromConfigDict(config, benchmark_name)

  def _CreateBenchmarkSpecFromConfigDict(self, config_dict, benchmark_name):
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(benchmark_name,
                                                            flag_values=FLAGS,
                                                            **config_dict)
    benchmark_module = next((b for b in linux_benchmarks.BENCHMARKS if
                             b.BENCHMARK_NAME == benchmark_name))
    return benchmark_spec.BenchmarkSpec(benchmark_module, config_spec, URI)

  def extractDictAFromB(self, A, B):  # assertDictContainsSubset deprecated
    return dict([(k, B[k]) for k in A.keys() if k in B.keys()])


class VpnServiceTestCase(BaseVPNServiceTest):

  def testVpnServiceConfig(self):
    FLAGS.use_vpn = True
    FLAGS.vpn_service_gateway_count = 1
    spec = self._CreateBenchmarkSpecFromYaml(DEFAULT_CFG)
    spec.ConstructVPNService()
    spec.ConstructVirtualMachines()
    #  test global flags
    self.assertTrue(spec.config.flags['use_vpn'])
    self.assertEqual(spec.config.flags['vpn_service_gateway_count'], 1)
    # test vpn_service flags
    self.assertTrue(hasattr(spec, 'vpn_service'))
    self.assertTrue(spec.vpn_service is not None)
    self.assertEqual(spec.vpn_service.tunnel_count, 1)
    self.assertEqual(spec.vpn_service.ike_version, 2)
    self.assertEqual(spec.vpn_service.routing, 'static')
    # test benchmark_spec attributes
    self.assertTrue(hasattr(spec, 'vpn_gateways'))
    self.assertTrue(spec.vpn_gateways is not None)
    # test unpickled values for above
    pspec = pickle.loads(pickle.dumps(spec))
    self.assertTrue(pspec.config.flags['use_vpn'])
    self.assertEqual(pspec.config.flags['vpn_service_gateway_count'], 1)
    self.assertTrue(hasattr(pspec, 'vpn_service'))
    self.assertTrue(pspec.vpn_service is not None)
    self.assertEqual(pspec.vpn_service.tunnel_count, 1)
    self.assertEqual(pspec.vpn_service.ike_version, 2)
    self.assertEqual(pspec.vpn_service.routing, 'static')
    self.assertTrue(hasattr(pspec, 'vpn_gateways'))
    self.assertTrue(pspec.vpn_gateways is not None)

  def testGetVPNGWPairs(self):
    vpn_gateways = {'vpngw-us-west1-0-None': None,
                    'vpngw-us-west1-1-None': None,
                    'vpngw-us-central1-0-None': None,
                    'vpngw-us-central1-1-None': None, }
    FLAGS.use_vpn = True
    FLAGS.vpn_service_gateway_count = 1
    spec = self._CreateBenchmarkSpecFromYaml(DEFAULT_CFG)
    spec.ConstructVPNService()
    pairs = spec.vpn_service.GetVPNGWPairs(vpn_gateways)
    self.assertEqual(len(pairs), 4)
    # test unpickled values
    pspec = pickle.loads(pickle.dumps(spec))
    ppairs = pspec.vpn_service.GetVPNGWPairs(vpn_gateways)
    self.assertEqual(len(ppairs), 4)


class TunnelConfigTestCase(BaseVPNServiceTest):

  def testTunnelConfigHash(self):
    FLAGS.run_uri = URI
    ep1 = {
        'name': 'ep1',
        'ip': '1.2.3.4',
        'cidr': '1.2.3.4/5',
        'require_target_to_init': False,
        'tunnel_id': '12345',
    }
    ep2 = {
        'name': 'ep2',
        'ip': '9.8.7.6',
        'cidr': '9.8.7.6/5',
        'require_target_to_init': False,
        'tunnel_id': '98765',
    }

    endpoints = [ep1, ep2]

    conf = {
        'tunnel_name': 'tun1',
        'ike_version': '3',
        'routing': 'static',
        'psk': 'private',
        'endpoints': endpoints
    }

    tunnel_config = TunnelConfig()
    tunnel_config2 = TunnelConfig()

    hash1 = tunnel_config.hash()
    hash2 = tunnel_config2.hash()
    self.assertEqual(hash1, hash2)

    tunnel_config.setConfig(**conf)
    hash3 = tunnel_config.hash()
    self.assertNotEqual(hash1, hash3)
