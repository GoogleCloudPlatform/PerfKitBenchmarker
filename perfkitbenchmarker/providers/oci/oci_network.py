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
"""Module containing classes related to OCI networking.

A PKB-created network consists of a VCN, an internet gateway that the VCN's
default route table routes 0.0.0.0/0 through, and one regional subnet. Ports
are opened by adding ingress rules to the VCN's default security list.
"""

import json
import logging
import re
import threading
from typing import Any

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import network
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.oci import flags as oci_flags
from perfkitbenchmarker.providers.oci import util

FLAGS = flags.FLAGS

DEFAULT_SUBNET_CIDR = '10.0.0.0/24'
ALL_SOURCES = '0.0.0.0/0'
ALL_PROTOCOLS = 'all'
ICMP = '1'
TCP = '6'
UDP = '17'


def _ToCamelCase(value: Any) -> Any:
  """Converts the kebab-case keys printed by the oci CLI to camelCase.

  The oci CLI prints resources with kebab-case keys but expects complex
  arguments such as --ingress-security-rules in camelCase. Null values are
  dropped so that rules can be compared with those built by MakeIngressRule.

  Args:
    value: A JSON value as returned by the oci CLI.

  Returns:
    The value with every dictionary key converted to camelCase.
  """
  if isinstance(value, dict):
    return {
        re.sub(r'-([a-z])', lambda m: m.group(1).upper(), k): _ToCamelCase(v)
        for k, v in value.items()
        if v is not None
    }
  if isinstance(value, list):
    return [_ToCamelCase(v) for v in value]
  return value


def MakeIngressRule(
    protocol: str,
    source: str = ALL_SOURCES,
    start_port: int | None = None,
    end_port: int | None = None,
) -> dict[str, Any]:
  """Returns a security list ingress rule in the format the oci CLI expects."""
  rule = {
      'source': source,
      'sourceType': 'CIDR_BLOCK',
      'protocol': protocol,
      'isStateless': False,
  }
  if start_port is not None and protocol in (TCP, UDP):
    options_key = 'tcpOptions' if protocol == TCP else 'udpOptions'
    rule[options_key] = {
        'destinationPortRange': {
            'min': start_port,
            'max': end_port or start_port,
        }
    }
  return rule


class OciVcn(resource.BaseResource):
  """An object representing an OCI Virtual Cloud Network."""

  def __init__(self, name: str, region: str, cidr: str):
    super().__init__()
    self.name = name
    self.region = region
    self.cidr = cidr
    self.id = None
    self.default_route_table_id = None
    self.default_security_list_id = None

  def _Create(self):
    """Creates the VCN."""
    response, _, _ = util.IssueOciCommand(
        [
            'network',
            'vcn',
            'create',
            '--display-name',
            self.name,
            '--cidr-blocks',
            json.dumps([self.cidr]),
            '--dns-label',
            util.MakeDnsLabel('vcn', FLAGS.run_uri),
            '--freeform-tags',
            util.MakeFormattedDefaultTags(),
            '--wait-for-state',
            'AVAILABLE',
        ]
        + util.GetCompartmentArgs(),
        self.region,
    )
    self._SetIdsFromResponse(response)

  def _SetIdsFromResponse(self, response: dict[str, Any]):
    data = response['data']
    self.id = data['id']
    self.default_route_table_id = data['default-route-table-id']
    self.default_security_list_id = data['default-security-list-id']

  def _Exists(self) -> bool:
    return bool(self.id) and util.ResourceExists(
        ['network', 'vcn', 'get', '--vcn-id', self.id], self.region
    )

  def _Delete(self):
    util.IssueOciCommand(
        [
            'network',
            'vcn',
            'delete',
            '--vcn-id',
            self.id,
            '--force',
            '--wait-for-state',
            'TERMINATED',
        ],
        self.region,
        raise_on_failure=False,
    )

  def LoadExisting(self):
    """Looks up an existing VCN by display name."""
    response, _, _ = util.IssueOciCommand(
        ['network', 'vcn', 'list', '--display-name', self.name]
        + util.GetCompartmentArgs(),
        self.region,
    )
    vcns = [
        vcn
        for vcn in (response or {}).get('data', [])
        if vcn['lifecycle-state'] == 'AVAILABLE'
    ]
    if not vcns:
      raise errors.Config.InvalidValue(
          f'No available VCN named {self.name} found in {self.region}.'
      )
    self._SetIdsFromResponse({'data': vcns[0]})
    self.cidr = vcns[0]['cidr-block']

  def GetSecurityListRules(self) -> list[dict[str, Any]]:
    """Returns the ingress rules of the default security list."""
    response, _, _ = util.IssueOciCommand(
        [
            'network',
            'security-list',
            'get',
            '--security-list-id',
            self.default_security_list_id,
        ],
        self.region,
    )
    return _ToCamelCase(response['data']['ingress-security-rules'])

  def UpdateSecurityListRules(self, rules: list[dict[str, Any]]):
    """Replaces the ingress rules of the default security list."""
    util.IssueOciCommand(
        [
            'network',
            'security-list',
            'update',
            '--security-list-id',
            self.default_security_list_id,
            '--ingress-security-rules',
            json.dumps(rules),
            '--force',
            '--wait-for-state',
            'AVAILABLE',
        ],
        self.region,
    )

  def UpdateRouteRules(self, rules: list[dict[str, Any]]):
    """Replaces the route rules of the default route table."""
    util.IssueOciCommand(
        [
            'network',
            'route-table',
            'update',
            '--rt-id',
            self.default_route_table_id,
            '--route-rules',
            json.dumps(rules),
            '--force',
            '--wait-for-state',
            'AVAILABLE',
        ],
        self.region,
    )


class OciInternetGateway(resource.BaseResource):
  """An object representing an OCI Internet Gateway."""

  def __init__(self, name: str, region: str, vcn: OciVcn):
    super().__init__()
    self.name = name
    self.region = region
    self.vcn = vcn
    self.id = None

  def _Create(self):
    response, _, _ = util.IssueOciCommand(
        [
            'network',
            'internet-gateway',
            'create',
            '--display-name',
            self.name,
            '--vcn-id',
            self.vcn.id,
            '--is-enabled',
            'true',
            '--freeform-tags',
            util.MakeFormattedDefaultTags(),
            '--wait-for-state',
            'AVAILABLE',
        ]
        + util.GetCompartmentArgs(),
        self.region,
    )
    self.id = response['data']['id']

  def _PostCreate(self):
    self.vcn.UpdateRouteRules([{
        'destination': ALL_SOURCES,
        'destinationType': 'CIDR_BLOCK',
        'networkEntityId': self.id,
    }])

  def _PreDelete(self):
    # A gateway cannot be deleted while a route rule targets it.
    self.vcn.UpdateRouteRules([])

  def _Exists(self) -> bool:
    return bool(self.id) and util.ResourceExists(
        ['network', 'internet-gateway', 'get', '--ig-id', self.id],
        self.region,
    )

  def _Delete(self):
    util.IssueOciCommand(
        [
            'network',
            'internet-gateway',
            'delete',
            '--ig-id',
            self.id,
            '--force',
            '--wait-for-state',
            'TERMINATED',
        ],
        self.region,
        raise_on_failure=False,
    )


class OciSubnet(resource.BaseResource):
  """An object representing a regional OCI subnet."""

  def __init__(self, name: str, region: str, vcn: OciVcn, cidr: str):
    super().__init__()
    self.name = name
    self.region = region
    self.vcn = vcn
    self.cidr = cidr
    self.id = None

  def _Create(self):
    response, _, _ = util.IssueOciCommand(
        [
            'network',
            'subnet',
            'create',
            '--display-name',
            self.name,
            '--vcn-id',
            self.vcn.id,
            '--cidr-block',
            self.cidr,
            '--dns-label',
            util.MakeDnsLabel('sub', FLAGS.run_uri),
            '--freeform-tags',
            util.MakeFormattedDefaultTags(),
            '--wait-for-state',
            'AVAILABLE',
        ]
        + util.GetCompartmentArgs(),
        self.region,
    )
    self.id = response['data']['id']

  def _Exists(self) -> bool:
    return bool(self.id) and util.ResourceExists(
        ['network', 'subnet', 'get', '--subnet-id', self.id], self.region
    )

  def _Delete(self):
    util.IssueOciCommand(
        [
            'network',
            'subnet',
            'delete',
            '--subnet-id',
            self.id,
            '--force',
            '--wait-for-state',
            'TERMINATED',
        ],
        self.region,
        raise_on_failure=False,
    )

  def LoadExisting(self):
    """Uses the first available subnet of an existing VCN."""
    response, _, _ = util.IssueOciCommand(
        ['network', 'subnet', 'list', '--vcn-id', self.vcn.id]
        + util.GetCompartmentArgs(),
        self.region,
    )
    subnets = [
        subnet
        for subnet in (response or {}).get('data', [])
        if subnet['lifecycle-state'] == 'AVAILABLE'
    ]
    if not subnets:
      raise errors.Config.InvalidValue(
          f'VCN {self.vcn.name} in {self.region} has no available subnets.'
      )
    self.id = subnets[0]['id']
    self.cidr = subnets[0]['cidr-block']


class OciNetwork(network.BaseNetwork):
  """Object representing an OCI network: a VCN, gateway and subnet."""

  CLOUD = provider_info.OCI

  def __init__(self, spec):
    super().__init__(spec)
    self.region = util.GetRegionFromZone(spec.zone)
    self.is_existing_network = bool(oci_flags.OCI_NETWORK_NAME.value)
    self.name = oci_flags.OCI_NETWORK_NAME.value or f'pkb-{FLAGS.run_uri}'
    self.vcn = OciVcn(self.name, self.region, oci_flags.OCI_VCN_CIDR.value)
    self.internet_gateway = OciInternetGateway(self.name, self.region, self.vcn)
    self.subnet = OciSubnet(
        self.name, self.region, self.vcn, spec.cidr or DEFAULT_SUBNET_CIDR
    )
    self._security_list_lock = threading.Lock()

  @classmethod
  def _GetKeyFromNetworkSpec(cls, spec):
    """VCNs and subnets are regional, so share them across a region's ADs."""
    return (cls.CLOUD, util.GetRegionFromZone(spec.zone))

  @property
  def subnet_id(self) -> str | None:
    return self.subnet.id

  def Create(self):
    """Creates the network, or looks it up if it already exists."""
    if self.is_existing_network:
      self.vcn.LoadExisting()
      self.subnet.LoadExisting()
      return
    self.vcn.Create()
    self.internet_gateway.Create()
    self.subnet.Create()
    # Like the default networks of other providers, allow all traffic between
    # instances in the VCN.
    self.AddIngressRules([MakeIngressRule(ALL_PROTOCOLS, self.vcn.cidr)])

  def Delete(self):
    """Deletes the network if PKB created it."""
    if self.is_existing_network:
      return
    self.subnet.Delete()
    self.internet_gateway.Delete()
    self.vcn.Delete()

  def AddIngressRules(self, rules: list[dict[str, Any]]):
    """Adds ingress rules to the VCN's default security list.

    Rules that are already present are not added again.

    Args:
      rules: Ingress rules as returned by MakeIngressRule.
    """
    with self._security_list_lock:
      current_rules = self.vcn.GetSecurityListRules()
      new_rules = [rule for rule in rules if rule not in current_rules]
      if new_rules:
        self.vcn.UpdateSecurityListRules(current_rules + new_rules)


class OciFirewall(network.BaseFirewall):
  """An object representing the OCI security list rules opened by PKB."""

  CLOUD = provider_info.OCI

  def __init__(self):
    self.firewall_set = set()
    self._lock = threading.Lock()

  def _AllowRules(self, vm, key, rules):
    """Adds rules to the VM's network once per key."""
    if vm.is_static:
      return
    if vm.network.is_existing_network:
      logging.warning(
          (
              'Not modifying the security list of existing VCN %s. Make sure it'
              ' allows the traffic this benchmark needs.'
          ),
          vm.network.name,
      )
      return
    with self._lock:
      if key in self.firewall_set:
        return
      vm.network.AddIngressRules(rules)
      self.firewall_set.add(key)

  def AllowPort(self, vm, start_port, end_port=None, source_range=None):
    """Opens TCP and UDP ports on the VM's network.

    Args:
      vm: The BaseVirtualMachine object to open the ports for.
      start_port: The first port in a range of ports to open.
      end_port: The last port in a range of ports to open. If None, only
        start_port will be opened.
      source_range: List of source CIDRs to allow for this port. If None, all
        sources are allowed.
    """
    end_port = end_port or start_port
    sources = tuple(source_range or [ALL_SOURCES])
    rules = [
        MakeIngressRule(protocol, source, start_port, end_port)
        for source in sources
        for protocol in (TCP, UDP)
    ]
    self._AllowRules(
        vm, (vm.network.region, start_port, end_port, sources), rules
    )

  def AllowIcmp(self, vm):
    """Opens the ICMP protocol on the VM's network."""
    self._AllowRules(
        vm, (vm.network.region, 'icmp'), [MakeIngressRule(ICMP, ALL_SOURCES)]
    )
