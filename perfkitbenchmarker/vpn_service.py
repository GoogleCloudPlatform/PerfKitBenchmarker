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
"""VPN support for network benchmarks.

This module contains the main VPNService class, which manages the VPN lifecycle,
the VPN class, which manages the VPN tunnel lifecycle between two endpoints, and
the TunnelConfig class, which maintains the parameters needed to configure a
tunnel between two endpoints. Related: perfkitbenchmarker.network
module includes the BaseVpnGateway class to manage VPN gateway endpoints.
"""

import itertools
import json
import logging
import re
import threading
import time
import uuid

from perfkitbenchmarker import context, vm_util
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import resource

flags.DEFINE_integer('vpn_service_tunnel_count', None,
                     'Number of tunnels to create for each VPN GW pair.')
flags.DEFINE_integer('vpn_service_gateway_count', None,
                     'Number of VPN GWs to create for each vm_group.')
flags.DEFINE_string('vpn_service_name', None,
                    'If set, use this name for VPN Service.')
flags.DEFINE_string('vpn_service_shared_key', None,
                    'If set, use this PSK for VPNs.')
flags.DEFINE_string('vpn_service_routing_type', None,
                    'static or dynamic(BGP)')
flags.DEFINE_integer('vpn_service_ike_version', None, 'IKE version')

FLAGS = flags.FLAGS


def GetVPNServiceClass():
  """Gets the VPNService class.

  Args:

  Returns:
    Implementation class
  """
  return resource.GetResourceClass(VPNService)


class VPN(object):
  """An object representing the VPN.

  A VPN instance manages tunnel configurations for exactly 1 pair of endpoints.
  """

  def __init__(self, *args, **kwargs):
      return object.__init__(self, *args, **kwargs)

  def getKeyFromGWPair(self, gateway_pair, suffix=''):
    """Return the VPN key for a pair of endpoints.

    Args:
      gateway_pair: A tuple of 2 VPN gateways which define the VPN tunnel.
      suffix: A unique suffix if multiple tunnels b/t this gateway pair exist.

    Returns:
      string. The VPN key.

    """
    key = 'vpn' + ''.join(gw for gw in gateway_pair) + suffix + FLAGS.run_uri
    return key

  def Create(self, gateway_pair, suffix=''):
    self.GWPair = gateway_pair
    self.name = self.getKeyFromGWPair(gateway_pair)
    self.tunnel_config = TunnelConfig(tunnel_name=self.name, suffix=suffix)

  def Delete(self):
    pass

  def GetVPN(self, gateway_pair, suffix=''):
    """Gets a VPN object for the gateway_pair or creates one if none exists

    Args:
    gateway_pair: a tuple of two VpnGateways
    """

    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('GetVPN called in a thread without a '
                         'BenchmarkSpec.')
    with benchmark_spec.vpns_lock:
      key = self.getKeyFromGWPair(gateway_pair, suffix)
      if key not in benchmark_spec.vpns:
        self.Create(gateway_pair, suffix)
        benchmark_spec.vpns[key] = self
      return benchmark_spec.vpns[key]

  def ConfigureTunnel(self):
    """Configure the VPN tunnel."""

    benchmark_spec = context.GetThreadBenchmarkSpec()
    vpn_gateway_0 = benchmark_spec.vpn_gateways[self.GWPair[0]]
    vpn_gateway_1 = benchmark_spec.vpn_gateways[self.GWPair[1]]

    assert not (vpn_gateway_0.require_target_to_init and vpn_gateway_1.require_target_to_init), 'Cant connect 2 passive VPN GWs'

    tunnel_config_hash = None
    while not self.isTunnelConfigured():
        vpn_gateway_0.ConfigureTunnel(self.tunnel_config)
        vpn_gateway_1.ConfigureTunnel(self.tunnel_config)
        if self.tunnel_config.hash() == tunnel_config_hash:
          raise ValueError('Not enough info to configure tunnel.')
        tunnel_config_hash = self.tunnel_config.hash()

    tunnel_status = self.isTunnelReady()
    logging.info('Tunnel is ready?: %s ' % tunnel_status)

  def isTunnelConfigured(self):
    """Returns True if the tunnel configuration is complete.

    Returns:
      boolean.
    """
    is_tunnel_configured = False
    if len(self.tunnel_config.endpoints) == 2:
      if self.tunnel_config.endpoints[self.GWPair[0]]['is_configured'] and self.tunnel_config.endpoints[self.GWPair[1]]['is_configured']:
        logging.info('Tunnel is configured.')
        is_tunnel_configured = True
    return is_tunnel_configured

  @vm_util.Retry(retryable_exceptions=errors.Resource.RetryableCreationError)
  def isTunnelReady(self):
    """ Returns True if the tunnel is up.

    Returns:
      boolean.
    """
    benchmark_spec = context.GetThreadBenchmarkSpec()
    logging.info('Tunnel endpoints configured. Waiting for tunnel...')
    ready = benchmark_spec.vpn_gateways[self.GWPair[0]].IsTunnelReady(self.tunnel_config.endpoints[self.GWPair[0]]['tunnel_id']) and benchmark_spec.vpn_gateways[self.GWPair[1]].IsTunnelReady(self.tunnel_config.endpoints[self.GWPair[1]]['tunnel_id'])
    if not ready:
      raise errors.Resource.RetryableCreationError()

    return ready


class TunnelConfig(object):
  """Object to hold all parms needed to configure a tunnel.

  tunnel_config =
  { tunnel_name = ''
    routing = ''
    psk = ''
    endpoints = [ep1={...}, ep2={...}
    }

  endpoint =
  { name = ''
    ip = ''
    cidr = ''
    require_target_to_init = t/f
    tunnel_id = ''

  }
  }

  """

  _tunnelconfig_lock = threading.Lock()

  def __init__(self, **kwargs):
    super(TunnelConfig, self).__init__()
    self.tunnel_name = kwargs.get('tunnel_name', 'unnamed_tunnel')
    self.endpoints = {}
    self.routing = kwargs.get('routing', 'static')
    self.ike_version = kwargs.get('ike_version', 2)
    self.shared_key = kwargs.get('shared_key', 'key' + FLAGS.run_uri)
    self.suffix = kwargs.get('suffix', '')

  def setConfig(self, **kwargs):
    with self._tunnelconfig_lock:
      for key in kwargs:
          setattr(self, key, kwargs[key])

  def __str__(self):
    return str(json.dumps(self.__dict__, sort_keys=True, default=str))

  def hash(self):
    """Hash the current tunnel config.

    Returns:
      int: An integer that changes if any properties have changes.

    """
    return hash(json.dumps(self.__dict__, sort_keys=True, default=str))


class VPNService(resource.BaseResource):
  """Service class to manage VPN lifecycle."""

  RESOURCE_TYPE = 'BaseVPNService'
  REQUIRED_ATTRS = ['SERVICE']

  def __init__(self, spec):
    """Initialize the VPN Service object.

    Args:
      vpn_service_spec: spec of the vpn service.
    """
    super(VPNService, self).__init__()
    self.name = spec.name
    self.tunnel_count = spec.tunnel_count
    self.gateway_count = FLAGS.vpn_service_gateway_count
    self.routing = spec.routing_type
    self.ike_version = spec.ike_version
    self.shared_key = spec.shared_key
    self.spec = spec
    self.vpns = {}
    self.vpn_properties = {'tunnel_count': self.tunnel_count,
                           'gateway_count': self.gateway_count,
                           'routing': self.routing,
                           'ike_version': self.ike_version,
                           'shared_key': self.shared_key,
                           }


  def GetResourceMetadata(self):
    """Returns a dictionary of metadata about the resource."""

    if not self.created:
      return {}
    result = self.metadata.copy()
    if self.routing is not None:
      result['vpn_service_routing_type'] = self.routing
    if self.ike_version is not None:
      result['vpn_service_ike_version'] = self.ike_version
    if self.tunnel_count is not None:
      result['vpn_service_tunnel_count'] = self.tunnel_count
    if self.gateway_count is not None:
      result['gateway_count'] = self.gateway_count
    # if self.psk is not None:  # probably don't want to publish this.
    #   result['vpn_service_shared_key'] = self.psk

    return result

  def _Create(self):
    """Creates VPN objects for VpnGateway pairs."""

    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('CreateVPN Service. called in a thread without a '
                         'BenchmarkSpec.')


    self.vpn_gateway_pairs = self.GetVpnGatewayPairs(benchmark_spec.vpn_gateways)


    for gateway_pair in self.vpn_gateway_pairs:
      # creates the vpn if it doesn't exist and registers in bm_spec.vpns
      suffix = self.GetNewSuffix()
      vpn_id = VPN().getKeyFromGWPair(gateway_pair, suffix)
      self.vpns[vpn_id] = VPN().GetVPN(gateway_pair, suffix)
      self.vpns[vpn_id].tunnel_config.setConfig(**self.vpn_properties)


    vm_util.RunThreaded(lambda vpn: self.vpns[vpn].ConfigureTunnel(), list(self.vpns.keys()))

  def _Delete(self):
    pass

  def GetNewSuffix(self):
    """Names for tunnels, fr's, routes, etc need to be unique.

    Returns:
      string. A random string value.
    """
    return format(uuid.uuid4().fields[1], 'x')

  def GetMetadata(self):
    """Return a dictionary of the metadata for VPNs created."""
    basic_data = {'vpn_service_name': self.name,
                  'vpn_service_routing_type': self.routing,
                  'vpn_service_ike_version': self.ike_version,
                  'vpn_service_tunnel_count': self.tunnel_count,
                  'vpn_service_gateway_count': self.gateway_count,
                  # 'vpn_service_psk': self.psk,
                  }
    return basic_data

  def GetVpnGatewayPairs(self, vpn_gateways):
    """Returns pairs of gateways to create VPNs between.

    Currently creates a pair between all non-matching region endpoints (mesh).
    --vpn_service_gateway_count flag dictates how many gateways are created in
    each vm_group(region).
    --vpn_service_tunnel_count flag dictates how many VPN tunnels to create for
    each gateway pair.
    @TODO Add more pairing strategies as needed.

    Args:
      vpn_gateways: The dict of gateways created.

    Returns:
      list. The list of tuples of gateway pairs to create VPNs for.

    """
    vpn_gateway_pairs = itertools.combinations(vpn_gateways, 2)
    r = re.compile(r"(?P<gateway_prefix>.*-.*-.*)?-(?P<gateway_tnum>[0-9])-(?P<run_id>.*)")

    def filterGateways(gateway_pair):
      return r.search(gateway_pair[0]).group('gateway_prefix') != r.search(
          gateway_pair[1]).group('gateway_prefix')
    return list(filter(filterGateways, vpn_gateway_pairs))
