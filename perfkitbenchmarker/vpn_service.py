# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
from perfkitbenchmarker import errors, context, resource, flags
import itertools
import re
from itertools import ifilter
import uuid
import time
import logging

flags.DEFINE_integer('vpn_service_tunnel_count', 1,
                     'Number of tunnels to create for each VPNGW pair.')
flags.DEFINE_string('vpn_service_name', None,
                    'If set, use this name for VPN Service.')
flags.DEFINE_string('vpn_service_shared_key', None,
                    'If set, use this PSK for VPNs.')
flags.DEFINE_string('vpn_service_routing_type', 'static',
                    'static or dynamic(BGP)')

FLAGS = flags.FLAGS


class VPN(object):
  """An object representing the VPN."""

  def __init__(self, *args, **kwargs):
      self.GWPair = None  # pair of vpngw's to create tunnel between
      self.name = None  # name of the vpn created
      self.routing = 'static'  # @TODO add to flags
      self.psk = None
      return object.__init__(self, *args, **kwargs)

  def getKeyFromGWPair(self, gwpair):
    key = 'vpn' + ''.join(gw for gw in gwpair) + FLAGS.run_uri
    return key

  def Create(self, gwpair):
    self.GWPair = gwpair
    self.name = self.getKeyFromGWPair(gwpair)

  def Delete(self):
    pass
#     benchmark_spec = context.GetThreadBenchmarkSpec()
#     if benchmark_spec is None:
#       raise errors.Error('DeleteVPN called in a thread without a '
#                          'BenchmarkSpec.')
#     for vpngw_key in self.GWPair:
#       vpngw = benchmark_spec.vpngws[vpngw_key]
#       vpngw.Delete()
#     with benchmark_spec.vpngws_lock:
#       key = self.getKeyFromGWPair(self.GWPair).copy()
#       if key in benchmark_spec.vpns:
#         benchmark_spec.vpns.pop(key)

  def GetVPN(self, gwpair):
    ''' gets a VPN object for the gwpair or creates one if none exists

    Args:
    gwpair: a tuple of two VPNGWs
    '''

    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('GetFirewall called in a thread without a '
                         'BenchmarkSpec.')
    with benchmark_spec.vpngws_lock:
      key = self.getKeyFromGWPair(gwpair)
      if key not in benchmark_spec.vpns:
        self.Create(gwpair)
        benchmark_spec.vpns[key] = self
      return benchmark_spec.vpns[key]

  def PreConfigureTunnel(self, suffix=''):
    ''' Since AWS requires target GW to be known before releasing it's own
    VPN GW IP we add a preconfig step here to sort this out
    '''
    benchmark_spec = context.GetThreadBenchmarkSpec()
    vpngw0 = benchmark_spec.vpngws[self.GWPair[0]]
    vpngw1 = benchmark_spec.vpngws[self.GWPair[1]]
    assert not (vpngw0.require_target_to_init and vpngw1.require_target_to_init), 'Cant connect 2 passive VPN GWs'
    if vpngw0.require_target_to_init or vpngw1.require_target_to_init:  # if at least one is passive
      if vpngw0.require_target_to_init:
        if vpngw1.IP_ADDR is None:
          vpngw1.AllocateIP()
        vpngw0.preConfig(vpngw1)
      else:
        if vpngw0.IP_ADDR is None:
          vpngw0.AllocateIP()
        vpngw1.preConfig(vpngw0)

#
#     for vpngw_key in self.GWPair:
#       vpngw = benchmark_spec.vpngws[vpngw_key]
#       # 1st set the gw public ip if we can
#       if not vpngw.require_target_to_init and vpngw.IP_ADDR is None:
#         vpngw.AllocateIP()
#

  def PostConfigureTunnel(self, suffix=''):
    ''' Since AWS requires target GW to be known before releasing it's own
    VPN GW IP we add a postconfig step here to sort this out
    '''
    benchmark_spec = context.GetThreadBenchmarkSpec()
    vpngw0 = benchmark_spec.vpngws[self.GWPair[0]]
    vpngw1 = benchmark_spec.vpngws[self.GWPair[1]]
    assert not (vpngw0.require_target_to_init and vpngw1.require_target_to_init), 'Cant connect 2 passive VPN GWs'
    if vpngw0.require_target_to_init or vpngw1.require_target_to_init:  # if at least one is passive
      if vpngw0.require_target_to_init:
        vpngw0.postConfig(vpngw1)
      else:
        vpngw1.postConfig(vpngw0)

  def ConfigureTunnel(self, suffix=''):

    # AWS workaround
    self.PreConfigureTunnel(suffix)

    #  @TODO thread this
    benchmark_spec = context.GetThreadBenchmarkSpec()
    for vpngw_key in self.GWPair:
      vpngw = benchmark_spec.vpngws[vpngw_key]
      if vpngw.IP_ADDR is None:
        vpngw.AllocateIP()

    self.PostConfigureTunnel(suffix)

    benchmark_spec.vpngws[self.GWPair[0]].SetupForwarding(suffix=suffix)
    benchmark_spec.vpngws[self.GWPair[1]].SetupForwarding(suffix=suffix)

    benchmark_spec.vpngws[self.GWPair[0]].SetupTunnel(
        benchmark_spec.vpngws[self.GWPair[1]], FLAGS.run_uri, suffix=suffix)
    benchmark_spec.vpngws[self.GWPair[1]].SetupTunnel(
        benchmark_spec.vpngws[self.GWPair[0]], FLAGS.run_uri, suffix=suffix)

    benchmark_spec.vpngws[self.GWPair[0]].SetupRouting(
        benchmark_spec.vpngws[self.GWPair[1]], suffix=suffix)
    benchmark_spec.vpngws[self.GWPair[1]].SetupRouting(
        benchmark_spec.vpngws[self.GWPair[0]], suffix=suffix)

    tunnel_status = self.isTunnelReady()
    logging.info('Tunnel is ready?: %s ' % tunnel_status)

  # tunnel should be up now, just wait for all clear
  # blocking here for now
  def isTunnelReady(self):
    benchmark_spec = context.GetThreadBenchmarkSpec()
    ready = False
    timeout = time.time() + 60 * 5  # give up after 5 mins
    while(not ready and time.time() < timeout):
      ready = benchmark_spec.vpngws[self.GWPair[0]].IsTunnelReady() and benchmark_spec.vpngws[self.GWPair[1]].IsTunnelReady()
      time.sleep(5)

    return ready


class VPNService(resource.BaseResource):

  def __init__(self, spec):
    """Initialize the VPN Service object.

    Args:
      vpn_service_spec: spec of the vpn service.
    """
    super(VPNService, self).__init__()
    self.shared_key = spec.vpn_service_spec.shared_key
    self.name = spec.vpn_service_spec.name
    self.tunnel_count = spec.vpn_service_spec.tunnel_count
    self.routing = FLAGS.vpn_service_routing_type  # static/dynamic
    self.psk = FLAGS.run_uri

    # update metadata
    self.metadata.update({'t_count': self.tunnel_count,
                          'vpn_name': self.name})

  def _Create(self):
    """Creates VPN objects for VPNGW pairs.
    """

    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('CreateVPN Service called in a thread without a '
                         'BenchmarkSpec.')
    # with benchmark_spec.vpngws_lock:
    self.vpngw_pairs = self.GetVPNGWPairs(benchmark_spec.vpngws)
    # with benchmark_spec.vpns_lock:
    for gwpair in self.vpngw_pairs:
      # creates the vpn if it doesn't exist and registers in bm_spec.vpns
      suffix = format(uuid.uuid4().fields[1], 'x')  # unique enough

      vpn = VPN().GetVPN(gwpair)
      vpn.psk = self.shared_key
      vpn.routing = self.routing  # @TODO need mixed static/dynamic tunnels?
      vpn.ConfigureTunnel(suffix=suffix)

  def _Delete(self):
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('CreateVPN Service called in a thread without a BenchmarkSpec.')
    for vpn in benchmark_spec.vpns:
      benchmark_spec.vpns[vpn].Delete()

  def GetMetadata(self):
    """Return a dictionary of the metadata for VPNs created."""
    basic_data = {'vpn_service': self.name,
                  'managed_vpns': self.name,
                  'tunnel_count': self.tunnel_count}
    # TODO grab this information for user_managed clusters.
    if self.tunnel_count > 1:
      basic_data.update({'ecmp_status': self.name})
    return basic_data

  def GetVPNGWPairs(self, vpngws):
    # vpngw-us-west1-0-28ed049a <-> vpngw-us-central1-0-28ed049a # yes
    # vpngw-us-west1-0-28ed049a <-> vpngw-us-central1-1-28ed049a # no
     # get all gw pairs then filter out the non matching tunnel id's
    vpngw_pairs = itertools.combinations(vpngws, 2)
    r = re.compile(r"(?P<gw_prefix>.*-.*-.*)?-(?P<gw_tnum>[0-9])-(?P<run_id>.*)")
    # function = lambda x: r.search(x[0]).group('gw_tnum') == r.search(x[1]).group('gw_tnum')
    function = lambda x: r.search(x[0]).group('gw_prefix') != r.search(x[1]).group('gw_prefix')
    return ifilter(function, vpngw_pairs)
