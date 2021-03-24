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
"""Module containing base rest api constructs to run on IBM Cloud."""

import datetime
import io
import itertools
import json
import logging
import os
import time
from typing import Any, Dict
import urllib.parse

from perfkitbenchmarker import vm_util
import requests

IMAGE_UPLOAD_ATTEMPTS = 5
SPEC_PATHS = [
    'public_gateways',
    'security_groups',
    'floating_ips',
    'network_acls'
]
PLURALS = {
    'bgpconf': 'bgpconf',
    'edgerouter': 'edgerouter',
    'policy': 'policies'
}

GENERATION = 'generation='
HTTP_TIMEOUT_CONNECT = 10
HTTP_TIMEOUT = 120


def RoundRobin(l1, l2):
  return list(next(it) for it in itertools.cycle([iter(l1), iter(l2)]))


def Plural(x):
  return PLURALS.get(x, x + 's')


class IbmCloudRequestError(Exception):
  pass


class IbmCloud:
  """Base object that handles IBM Cloud REST api call requests."""

  def __init__(self, endpoint=None, url=None, account=None, apikey=None,
               vm_creator=False, token=None, verbose=False, version='v1',
               silent=False, force=False, trace=False):

    # For Token
    self._endpoint = endpoint or os.environ.get('IBMCLOUD_AUTH_ENDPOINT')
    self._url = url or os.environ.get('IBMCLOUD_ENDPOINT')
    self._acct = account or os.environ.get('IBMCLOUD_ACCOUNT_ID')
    self._apikey = apikey or os.environ.get('IBMCLOUD_APIKEY')
    self._token = token or os.environ.get('IBMCLOUD_TOKEN')
    self._token_time = time.time()
    self.version = version
    self._verbose = verbose
    self._silent = silent
    self._force = force
    self._trace = trace
    self._vm_creator = vm_creator
    self._generation = ('version=' + str(datetime.datetime.now().date()) + '&' +
                        GENERATION +
                        (os.environ.get('IBMCLOUD_GENERATION') or '2'))

    if not self._url:
      raise IbmCloudRequestError(
          'url has to specified either in the initialization of the '
          'client or via an environment variable (\"IBMCLOUD_ENDPOINT\")')
    if not self._acct:
      raise IbmCloudRequestError(
          'acct has to specified either in the initialization of the '
          'client or via an environment variable (\"IBMCLOUD_ACCOUNT_ID\")')

    if not self._apikey:
      raise IbmCloudRequestError(
          'apikey has to specified either in the initialization of the '
          'client or via an environment variable (\"IBMCLOUD_APIKEY\")')

    parsed_url = urllib.parse.urlparse(self._url)
    self._netloc = parsed_url.netloc or parsed_url.path.split('/', 1)[0]
    self._scheme = parsed_url.scheme or 'http'
    self._search_accts = [self._acct, None, 'system']

    # new token if not set, force set or if running for more than 50 min
    if (not self._token or self._force or self._token_time <
        (time.time() - 3000)):
      self.SetToken()

  @vm_util.Retry(max_retries=3, timeout=vm_util.DEFAULT_TIMEOUT)
  def GetToken(self):
    """Get a user token."""
    token = None
    req_data = {
        'grant_type': 'urn:ibm:params:oauth:grant-type:apikey',
        'response_type': 'cloud_iam',
        'apikey': self._apikey
    }
    count = 1
    while token is None:
      if not self._silent:
        logging.info('Sending a POST request to get a token: %s',
                     self._endpoint)
      resp = requests.request(
          'POST',
          self._endpoint,
          data=req_data,
          headers=None,
          timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT))
      if resp is not None:
        if self._verbose:
          logging.info('response=%s', str(resp))
          if resp.text:
            logging.info('response text=%s', resp.text)
        if resp.status_code >= 200 and resp.status_code <= 299:
          resp = json.loads(resp.text)
          if 'access_token' in resp:
            token = resp['access_token']
            if self._verbose:
              logging.info('token: %s', token)
            break
        else:
          logging.error('POST: %s, response status_code: %s, resp: %s',
                        self._endpoint, resp.status_code, str(resp))
      else:
        logging.info('Request POST: %s, no response returned', self._endpoint)
      count += 1
      if count > 3:
        break
      time.sleep(5)
    return token

  def Token(self):
    """Returns the token."""
    return self._token

  def SetToken(self):
    """Gets a new token."""
    try:
      self._token = self.GetToken()
      self._token_time = time.time()
    except Exception as err:
      raise IbmCloudRequestError('Authorization failed: %s' % err)

  def Request(self,
              method,
              path,
              data=None,
              headers=None,
              timeout=None,
              session=None):
    """Constructs base rest api calls to run against IBM Cloud regional api server.

    Args:
      method: one of GET, POST, PUT, PATCH or DELETE.
      path: rest api uri.
      data: body data.
      headers: header data if any.
      timeout: timeout for the request
      session: session object if available.

    Returns:
      request response.
    """
    path = f'/{self.version}/{path}'

    if 'limit' in path:
      path += '&' + self._generation
    else:
      path += '?' + self._generation

    h = {'Authorization': 'Bearer ' + self._token}
    if self._trace:
      h['trace'] = 'on'
    if data is not None and not isinstance(data, io.IOBase):
      h['Content-Type'] = 'application/json'
      data = json.dumps(data)
    if headers:
      h.update(headers)
    if self._verbose:
      h['trace'] = 'on'
      curl = '>>> curl -X %s' % method
      for k, v in h.items():
        curl += ' -H "%s: %s"' % (k, v)
      if data:
        curl += ' -d ' + '"' + str(data).replace('"', '\\"') + '"'
      curl += ' %s://%s%s' % (self._scheme, self._netloc, path)
      logging.info(curl)
      if data:
        logging.info('==== DATA ====')
        logging.info(data)

    res = None
    response_code = None
    response_text = None
    request_id = None
    try:
      data_timeout = timeout if timeout is not None else HTTP_TIMEOUT
      if session is None:
        res = requests.request(
            method,
            self._url + path,
            data=data,
            headers=h,
            timeout=(HTTP_TIMEOUT_CONNECT, data_timeout))  # verify=False
      else:
        res = session.request(
            method,
            self._url + path,
            data=data,
            headers=h,
            timeout=(HTTP_TIMEOUT_CONNECT, data_timeout))
      if res is not None:
        request_id = (
            res.headers['X-Request-Id']
            if 'X-Request-Id' in res.headers else None)
        if self._verbose:
          logging.info('Request %s:, %s, request id: %s ', method, path,
                       request_id)
          logging.info('response=%s', str(res))
          if res.text:
            logging.info('response text=%s', res.text)
        response_code = res.status_code
        if res.status_code < 200 or res.status_code > 299:
          logging.error('Request %s:, %s, response status_code: %s, res: %s',
                        method, path, res.status_code, str(res))
          try:
            if 'errors' in res.text:
              restext = json.loads(res.text)
              logging.info('    *** %s -- %s\n\n', restext['errors'][0]['code'],
                           restext['errors'][0]['message'])
          except Exception:  # pylint: disable=broad-except
            pass
          response_text = str(res.text)
      else:
        logging.info('Request %s: %s, no response returned', method, path)
    except requests.exceptions.RequestException as e:
      logging.exception('RequestException: %s', e)

    if res and res.status_code != 204:
      try:
        res = json.loads(res.text)
      except:
        raise IbmCloudRequestError('Request %s failed: %s' % (path, str(res)))

      if self._verbose:
        logging.info('==== RESPONSE ====')
        logging.info(json.dumps(str(res), indent=2))

    if self._vm_creator:
      return res, response_code, response_text, request_id
    else:
      return res

  def __getattr__(self, name):
    """Handles non standard action method names with underscores."""
    if '_' not in name:
      raise AttributeError(name)

    action, path = name.split('_', 1)
    if path in SPEC_PATHS:
      paths = path
    else:
      paths = path.split('_')

    if action in ['create', 'show', 'delete']:
      if paths in SPEC_PATHS:
        paths = [paths]  # must be a list
      else:
        paths = [Plural(p) for p in paths]
    else:
      paths = [Plural(p) for p in paths[:-1]] + [paths[-1]]

    if action == 'create':

      def Create(*args, **kwargs):
        path = '/'.join(RoundRobin(paths, args[:-1]))
        return self.Request('POST', path, args[-1], **kwargs)

      return Create

    if action in ['list', 'show']:

      def Show(*args, **kwargs):
        path = '/'.join(RoundRobin(paths, args))
        return self.Request('GET', path, **kwargs)

      return Show

    if action == 'delete':

      def Delete(*args, **kwargs):
        path = '/'.join(RoundRobin(paths, args))
        return self.Request('DELETE', path, **kwargs)

      return Delete

    raise AttributeError(name)


class BaseManager:
  """Base class that handles IBM Cloud REST api call requests.

    Use the derived classes for each resource type.
  """
  # This value should be overwritten by derived classes.
  _type = 'base'

  def __init__(self, genesis):
    self._g = genesis

  def GetUri(self, item) -> str:
    return '%ss' % self._type + '/' + item

  def GetUris(self, item):
    return '%s' % self._type + '/' + item

  def GetProfileUri(self):
    return '%s' % self._type + '/profiles'

  def Create(self, **kwargs):
    create_method = getattr(self._g, 'create_%s' % self._type)
    return create_method(kwargs)

  def List(self, **kwargs):
    list_method = getattr(self._g, 'list_%ss' % self._type)
    return list_method(**kwargs) or {}

  def Show(self, item):
    item_uri = self.GetUri(item)
    return self._g.Request('GET', item_uri)

  def Delete(self, item):
    item_uri = self.GetUri(item)
    return self._g.Request('DELETE', item_uri)

  def SetToken(self):
    self._g.set_token()

  def Token(self):
    return self._g.Token()


class InstanceManager(BaseManager):
  """Handles requests on instances."""

  _type = 'instance'

  def Create(self, name, imageid, profile, vpcid, zone=None, key=None,
             subnet=None, networks=None, resource_group=None,
             iops=None, capacity=None, user_data=None,
             session=None, **kwargs) -> Dict[str, Any]:
    """Construct and send a vm create request.

    Args:
       name: name of the vm.
       imageid: image id to use.
       profile: machine_type.
       vpcid: vpc id.
       zone: zone name.
       key: regional ssh key id.
       subnet: subnet id.
       networks: optional. additional subnet ids.
       resource_group: optional.
       iops: iops on the boot volume.
       capacity: boot volume size.
       user_data: user data for windows.
       session: session id if used.
       **kwargs: additonal args.

    Returns:
      request reponse.

    Raises:
      IbmCloudRequestError: if instance not found.
    """
    req_data = {
        'name': name,
        'image': {
            'id': imageid
        },
        'vpc': {
            'id': vpcid
        },
        'profile': {
            'name': profile
        }
    }
    if zone:
      req_data['zone'] = {'name': zone}
    if key:
      req_data['keys'] = [{'id': key}]
    if resource_group:
      req_data['resource_group'] = [{'id': resource_group}]
    if subnet:
      req_data['primary_network_interface'] = {'subnet': {'id': subnet}}
    if networks:
      req_data['network_interfaces'] = []
      networkids = [str(item) for item in networks.split(',')]
      for subnet in networkids:
        req_data['network_interfaces'].append({'subnet': {'id': subnet}})

    bootvol_attr = {}  # attributes for the boot volume
    keycrn = kwargs.get('encryption_key', None)
    if capacity:
      bootvol_attr['capacity'] = capacity
    if iops:
      bootvol_attr['iops'] = iops
    if keycrn:
      bootvol_attr['encryption_key'] = {'crn': keycrn}
    if bootvol_attr:
      bootvol_attr['profile'] = {'name': 'general-purpose'}
      req_data['boot_volume_attachment'] = {'volume': bootvol_attr}

    data_volume = kwargs.get('external_data_volume', None)
    if data_volume:
      req_data['volume_attachments'] = [{'volume': {'id': data_volume}}]
    if user_data:
      req_data['user_data'] = user_data

    instance = self._g.create_instance(req_data, session=session)
    if not instance:
      raise IbmCloudRequestError('Failed to create instance')
    return instance

  def Start(self, instance) -> Dict[str, Any]:
    """Send a vm start request."""
    inst_uri = self.GetUri(instance) + '/actions'
    return self._g.Request('POST', inst_uri, {'type': 'start'})

  def Show(self, instance) -> Dict[str, Any]:
    """Send a vm get request."""
    inst_uri = self.GetUri(instance)
    return self._g.Request('GET', inst_uri)

  def ShowPolling(self,
                  instance,
                  timeout_polling=HTTP_TIMEOUT_CONNECT,
                  session=None) -> Dict[str, Any]:
    """Send a vm get request."""
    inst_uri = self.GetUri(instance)
    return self._g.Request(
        'GET', inst_uri, timeout=timeout_polling, session=session)

  def ShowInitialization(self, instance) -> Dict[str, Any]:
    """Send a vm get initialization request."""
    inst_uri = self.GetUri(instance) + '/initialization'
    return self._g.Request('GET', inst_uri)

  def Delete(self, instance):
    """Send a vm delete request."""
    inst_uri = self.GetUri(instance)
    return self._g.Request('DELETE', inst_uri)

  def Stop(self, instance, force=False) -> Dict[str, Any]:
    """Send a vm stop request."""
    inst_uri = self.GetUri(instance) + '/actions'
    req_data = {'type': 'stop', 'force': force}
    return self._g.Request('POST', '%s' % inst_uri, req_data)

  def Reboot(self, instance, force=False) -> Dict[str, Any]:
    """Send a vm reboot request."""
    inst_uri = self.GetUri(instance) + '/actions'
    req_data = {'type': 'reboot', 'force': force}
    return self._g.Request('POST', inst_uri, req_data)

  def ShowVnic(self, instance, vnicid) -> Dict[str, Any]:
    """Send a vm vnic get request."""
    inst_uri = self.GetUri(instance) + '/network_interfaces/' + vnicid
    return self._g.Request('GET', inst_uri)

  def CreateVnic(self, instance, name, subnet) -> Dict[str, Any]:
    """Send a vm vnic create request."""
    inst_uri = self.GetUri(instance) + '/network_interfaces'
    return self._g.Request('POST', inst_uri, {
        'name': name,
        'subnet': {
            'id': subnet
        }
    })

  def ListVnics(self, instance) -> Dict[str, Any]:
    """Send a vm vnic list request."""
    inst_uri = self.GetUri(instance) + '/network_interfaces'
    return self._g.Request('GET', inst_uri)

  def DeleteVnic(self, instance, vnicid):
    """Send a vm vnic delete request."""
    inst_uri = self.GetUri(instance) + '/network_interfaces/' + vnicid
    return self._g.Request('DELETE', inst_uri)

  def CreateVolume(self, instance, name, volume, delete) -> Dict[str, Any]:
    """Send a volume create request on a vm."""
    inst_uri = self.GetUri(instance) + '/volume_attachments'
    return self._g.Request(
        'POST', '%s' % inst_uri, {
            'delete_volume_on_instance_delete': delete,
            'name': name,
            'volume': {
                'id': volume
            }
        })

  def ListVolumes(self, instance) -> Dict[str, Any]:
    """Send a volume list request on a vm."""
    inst_uri = self.GetUri(instance) + '/volume_attachments'
    return self._g.Request('GET', inst_uri)

  def ShowVolume(self, instance, volume_attachment) -> Dict[str, Any]:
    """Send a volume get request on a vm."""
    inst_uri = (
        self.GetUri(instance) + '/volume_attachments/' + volume_attachment)
    return self._g.Request('GET', inst_uri)

  def DeleteVolume(self, instance, volume_attachment):
    """Send a volume delete request on a vm."""
    inst_uri = (
        self.GetUri(instance) + '/volume_attachments/' + volume_attachment)
    return self._g.Request('DELETE', inst_uri)

  def ListProfiles(self) -> Dict[str, Any]:
    """Send a vm profiles list request."""
    return self._g.Request('GET', '%s' % self.GetProfileUri())

  def List(self, next_start=None, session=None) -> Dict[str, Any]:
    """Send a vm list request."""
    inst_uri = '/' + self._g.version + '/instances?limit=100'
    if next_start:
      inst_uri += '&start=' + next_start
    return self._g.Request('GET', inst_uri, session=session)


class VolumeManager(BaseManager):
  """Handles requests on volumes."""

  _type = 'volume'

  def Create(self, zone, **kwargs) -> Dict[str, Any]:
    """Construct and send a vm create request.

    Args:
      zone: zone name.
      **kwargs:
        name - volume name.
        profile - volume profile.
        capacity - boot volume size.
        iops - iops on the volume.
        resource_group - optional.
        encryption_key - key to encrypt, optional.

    Returns:
      created volume.
    """
    req_data = {
        'zone': {
            'name': zone
        },
        'profile': {
            'name': kwargs.get('profile', 'general-purpose')
        }
    }
    if kwargs.get('capacity', None):
      req_data['capacity'] = kwargs.get('capacity')
    if kwargs.get('iops', None):
      req_data['iops'] = kwargs.get('iops')
    if kwargs.get('name', None):
      req_data['name'] = kwargs.get('name')
    if kwargs.get('resource_group', None):
      req_data['resource_group'] = {'id': kwargs.get('resource_group')}
    if kwargs.get('encryption_key', None):
      req_data['encryption_key'] = {'crn': kwargs.get('encryption_key')}
    return self._g.create_volume(req_data)

  def Show(self, vol_id) -> Dict[str, Any]:
    """Send a volume get request."""
    return self._g.Request('GET', 'volumes/%s' % vol_id)

  def Delete(self, vol_id):
    """Send a volume delete request."""
    return self._g.Request('DELETE', 'volumes/%s' % vol_id)

  def List(self, next_start=None) -> Dict[str, Any]:
    """Send a volume list request."""
    uri = '/' + self._g.version + '/volumes?limit=100'
    if next_start:
      uri += '&start=' + next_start
    return self._g.Request('GET', uri)

  def ListProfiles(self) -> Dict[str, Any]:
    """Send a volume profile list request."""
    return self._g.Request('GET', 'volume/profiles')


class VPCManager(BaseManager):
  """Handles requests on VPC."""

  _type = 'vpc'

  def Create(self, name) -> Dict[str, Any]:
    """Construct and send a vm create request.

    Args:
       name: name of the vpc.

    Returns:
      returns created vpc.
    """
    req_data = {'name': name}
    return self._g.create_vpc(req_data)

  def Show(self, vpc) -> Dict[str, Any]:
    """Send a vpc get request."""
    return self._g.Request('GET', 'vpcs/%s' % vpc)

  def Delete(self, vpc):
    """Send a vpc delete request."""
    return self._g.Request('DELETE', 'vpcs/%s' % vpc)

  def CreatePrefix(self, vpc, zone, cidr, name=None) -> Dict[str, Any]:
    """Send a vpc address prefix create request."""
    req_data = {'zone': {'name': zone}, 'cidr': cidr}
    if name:
      req_data['name'] = name
    vpc_uri = self.GetUri(vpc) + '/address_prefixes'
    return self._g.Request('POST', '%s' % vpc_uri, req_data)

  def ListPrefix(self, vpc) -> Dict[str, Any]:
    """Send a vpc address prefix list request."""
    return self._g.Request('GET', 'vpcs/%s/address_prefixes' % vpc)

  def DeletePrefix(self, vpc, prefix):
    """Send a vpc address prefix delete request."""
    return self._g.Request('DELETE',
                           'vpcs/%s/address_prefixes/%s' % (vpc, prefix))

  def PatchPrefix(self,
                  vpc,
                  prefix,
                  default=False,
                  name=None) -> Dict[str, Any]:
    """Send a vpc address prefix patch request."""
    req_data = {'is_default': default}
    if name:
      req_data['name'] = name
    return self._g.Request('PATCH',
                           'vpcs/%s/address_prefixes/%s' % (vpc, prefix),
                           req_data)

  def CreateRoutingTable(self,
                         vpc,
                         name,
                         routes=None,
                         session=None) -> Dict[str, Any]:
    """Send a vpc routing table create request."""
    req_data = {'name': name}
    if routes:
      req_data['routes'] = routes
    vpc_uri = self.GetUri(vpc) + '/routing_tables'
    return self._g.Request('POST', '%s' % vpc_uri, req_data, session=session)

  def ShowRoutingTable(self, vpc, routing, session=None) -> Dict[str, Any]:
    """Send a vpc routing table get request."""
    return self._g.Request(
        'GET', 'vpcs/%s/routing_tables/%s' % (vpc, routing), session=session)

  def PatchRoutingTable(self,
                        vpc,
                        routing,
                        ingress,
                        flag,
                        session=None) -> Dict[str, Any]:
    """Send a vpc routing table patch request."""
    vpc_uri = self.GetUri(vpc) + '/routing_tables/' + routing
    return self._g.Request('PATCH', vpc_uri, {ingress: flag}, session=session)

  def DeleteRoutingTable(self, vpc, routing, session=None):
    """Send a vpc routing table delete request."""
    vpc_uri = self.GetUri(vpc) + '/routing_tables/' + routing
    return self._g.Request('DELETE', '%s' % vpc_uri, session=session)

  def ListRoutingTable(self, vpc, session=None) -> Dict[str, Any]:
    """Send a vpc routing table list request."""
    return self._g.Request(
        'GET', 'vpcs/%s/routing_tables?limit=100' % vpc, session=session)

  def CreateRoute(self,
                  vpc,
                  routing,
                  name,
                  zone,
                  action,
                  destination,
                  nexthop=None,
                  session=None) -> Dict[str, Any]:
    """Send a vpc route create request."""
    req_data = {
        'name': name,
        'action': action,
        'destination': destination,
        'zone': {
            'name': zone
        }
    }
    if nexthop:
      req_data['next_hop'] = {'address': nexthop}
    vpc_uri = self.GetUri(vpc) + '/routing_tables/' + routing + '/routes'
    return self._g.Request('POST', '%s' % vpc_uri, req_data, session=session)

  def DeleteRoute(self, vpc, routing, route, session=None):
    """Send a vpc route delete request."""
    vpc_uri = self.GetUri(
        vpc) + '/routing_tables/' + routing + '/routes/' + route
    return self._g.Request('DELETE', '%s' % vpc_uri, session=session)

  def ListRoute(self, vpc, routing, session=None) -> Dict[str, Any]:
    """Send a vpc route list request."""
    return self._g.Request(
        'GET',
        'vpcs/%s/routing_tables/%s/routes?limit=100' % (vpc, routing),
        session=session)


class SGManager(BaseManager):
  """Handles requests on security groups."""

  _type = 'security_groups'

  def Create(self, resource_group, vpcid, **kwargs) -> Dict[str, Any]:
    """Construct and send a security group create request.

    Args:
       resource_group: optional.
       vpcid: vpc id.
       **kwargs:
         name - name of the vm.

    Returns:
      returns created security groups.
    """
    req_data = {'vpc': {'id': vpcid}}
    if resource_group:
      req_data['resource_group'] = {
          'id': resource_group
          }
    if kwargs.get('name', None):
      req_data['name'] = kwargs.get('name', None)

    return self._g.create_security_groups(req_data)

  def List(self) -> Dict[str, Any]:
    """Send a security group list request."""
    return self._g.Request('GET', self._type)

  def Show(self, sg) -> Dict[str, Any]:
    """Send a security group get request."""
    sg_id = self.GetUris(sg)
    return self._g.Request('GET', sg_id)

  def Delete(self, sg):
    """Send a security group delete request."""
    sg_id = self.GetUris(sg)
    return self._g.Request('DELETE', sg_id)

  def ShowRule(self, sg, ruleid) -> Dict[str, Any]:
    """Send a security group rule get request."""
    sg_uri = self.GetUris(sg) + '/rules/' + ruleid
    return self._g.Request('GET', sg_uri)

  def ListRules(self, sg) -> Dict[str, Any]:
    """Send a security group rule list request."""
    sg_uri = self.GetUris(sg) + '/rules'
    return self._g.Request('GET', sg_uri)

  def CreateRule(self,
                 sg,
                 direction,
                 ip_version,
                 cidr_block,
                 protocol,
                 port,
                 port_min=None,
                 port_max=None) -> Dict[str, Any]:
    """Send a security group rule create request.

    Args:
       sg: security group.
       direction: in or outbound.
       ip_version: ipv4 or 6.
       cidr_block: cidr_block.
       protocol: tcp or udp.
       port: port.
       port_min: port min.
       port_max: port max.

    Returns:
      returns request response.
    """
    sg_uri = self.GetUris(sg) + '/rules'
    req_data = {
        'direction': direction,
        'ip_version': ip_version,
        'remote': {
            'cidr_block': cidr_block
        },
        'protocol': protocol
    }
    if port:
      req_data['port_min'] = port
      req_data['port_max'] = port
    elif port_min and port_max:
      req_data['port_min'] = port_min
      req_data['port_max'] = port_max
    return self._g.Request('POST', sg_uri, req_data)

  def DeleteRule(self, sg, ruleid):
    """Send a security group rule delete request."""
    sg_uri = self.GetUris(sg) + '/rules/' + ruleid
    return self._g.Request('DELETE', '%s' % sg_uri)


class PGManager(BaseManager):
  """Handles requests on public gateways."""

  _type = 'public_gateways'

  def Create(self, resource_group, vpcid, zone, **kwargs) -> Dict[str, Any]:
    """Construct and send a vm create request.

    Args:
      resource_group: optional.
      vpcid: vpc id.
      zone: zone name.
      **kwargs:
        name - name of the vm.
        floating_ip - optional, floating ip id.

    Returns:
      returns created public gateways.
    """
    req_data = {'vpc': {'id': vpcid}, 'zone': {'name': zone}}
    if resource_group:
      req_data['resource_group'] = {
          'id': resource_group
          }
    if kwargs.get('name', None):
      req_data['name'] = kwargs.get('name', None)
    if kwargs.get('floating_ip', None):
      req_data['floating_ip'] = {'id': kwargs.get('floating_ip')}
    return self._g.create_public_gateways(req_data)

  def List(self) -> Dict[str, Any]:
    """Send a public gateway list request."""
    return self._g.Request('GET', self._type)

  def Show(self, pg) -> Dict[str, Any]:
    """Send a public gateway get request."""
    pg_id = self.GetUris(pg)
    return self._g.Request('GET', pg_id)

  def Delete(self, pg):
    """Send a public gateway delete request."""
    pg_id = self.GetUris(pg)
    return self._g.Request('DELETE', pg_id)


class RegionManager(BaseManager):
  """Handles requests on regions."""

  _type = 'region'

  def Show(self, region) -> Dict[str, Any]:
    """Send a region get request."""
    return self._g.Request('GET', 'regions/%s' % region)


class KeyManager(BaseManager):
  """Handles requests on ssh keys."""

  _type = 'keys'

  def Create(self, key, key_type, **kwargs) -> Dict[str, Any]:
    """Construct and send a ssh key create request.

    Args:
       key: public key string.
       key_type: rsa.
       **kwargs:
         name - name of the key:
         resource_group - optional.

    Returns:
      returns created key.
    """
    req_data = {'public_key': key, 'type': key_type}
    if kwargs.get('name', None):
      req_data['name'] = kwargs.get('name', None)
    if kwargs.get('resource_group', None):
      req_data['resource_group'] = {'id': kwargs.get('resource_group', None)}
    return self._g.create_key(req_data)

  def List(self) -> Dict[str, Any]:
    """Send a key list request."""
    return self._g.Request('GET', self._type)

  def Show(self, key) -> Dict[str, Any]:
    """Send a key get request."""
    key_id = self.GetUris(key)
    return self._g.Request('GET', key_id)

  def Delete(self, key):
    """Send a key delete request."""
    key_id = self.GetUris(key)
    return self._g.Request('DELETE', key_id)


class NetworkAclManager(BaseManager):
  """Handles requests on network acls."""

  _type = 'network_acls'

  def Create(self, name) -> Dict[str, Any]:
    """Construct and send a vm create request.

    Args:
       name: name of the vm.

    Returns:
      returns created network.
    """
    req_data = {'name': name}
    return self._g.create_network_acls(req_data)

  def List(self) -> Dict[str, Any]:
    """Send a network_acl list request."""
    return self._g.Request('GET', 'network_acls')

  def Show(self, network_acl) -> Dict[str, Any]:
    """Send a network_acl get request."""
    network_acl_id = self.GetUris(network_acl)
    return self._g.Request('GET', network_acl_id)

  def Delete(self, network_acl):
    """Send a network_acl delete request."""
    network_acl_id = self.GetUris(network_acl)
    return self._g.Request('DELETE', network_acl_id)


class SubnetManager(BaseManager):
  """Handles requests on subnets."""

  _type = 'subnet'

  def Create(self, subnet, vpcid, **kwargs) -> Dict[str, Any]:
    """Construct and send a vm create request.

    Args:
      subnet: subnet
      vpcid: vpc id.
      **kwargs:
        name - name of the subnet.
        zone - zone name.
        ip_version - ipv4 or 6.
        ipv4_cidr_block - cidr for subnet.

    Returns:
      created subnet.
    """
    req_data = {
        'ipv4_cidr_block': subnet,
        'vpc': {
            'id': vpcid
        },
        'ip_version': 'ipv4',
    }
    if kwargs.get('name'):
      req_data['name'] = kwargs.get('name')
    if kwargs.get('zone'):
      req_data['zone'] = {'name': kwargs.get('zone')}
    return self._g.create_subnet(req_data)

  def Show(self, subnet) -> Dict[str, Any]:
    """Send a subnet get request."""
    subnet_id = self.GetUri(subnet)
    return self._g.Request('GET', subnet_id)

  def Delete(self, subnet):
    """Send a subnet delete request."""
    subnet_id = self.GetUri(subnet)
    return self._g.Request('DELETE', subnet_id)

  def Patch(self, **kwargs) -> Dict[str, Any]:
    """Send a subnet patch request."""
    subnet_id = self.GetUri(kwargs.get('subnet'))
    return self._g.Request('PATCH', subnet_id, kwargs)

  def ShowPg(self, subnet) -> Dict[str, Any]:
    """Send a subnet public gateway get request."""
    uri = self.GetUri(subnet) + '/public_gateway'
    return self._g.Request('GET', uri)

  def AttachPg(self, subnet, pgid) -> Dict[str, Any]:
    """Send a subnet public gateway attach request."""
    uri = self.GetUri(subnet) + '/public_gateway'
    req_data = {'id': pgid}
    return self._g.Request('PUT', uri, req_data)

  def DeletePg(self, subnet):
    """Send a subnet public gateway delete request."""
    uri = self.GetUri(subnet) + '/public_gateway'
    return self._g.Request('DELETE', uri)


class ImageManager(BaseManager):
  """Handles requests on images."""

  _type = 'image'

  def Create(self, href, osname, name=None) -> Dict[str, Any]:
    """Construct and send a vm create request.

    Args:
       href: href to the image.
       osname: os name.
       name: name of the image.

    Returns:
      created image.
    """
    req_data = {'file': {'href': href}}
    if name is not None:
      req_data['name'] = name
    if osname is not None:
      req_data['operating_system'] = {'name': osname}
    return self._g.create_image(req_data)

  def Show(self, img) -> Dict[str, Any]:
    """Send a image get request."""
    img_id = self.GetUri(img)
    return self._g.Request('GET', img_id)

  def Delete(self, img):
    """Send a image delete request."""
    img_id = self.GetUri(img)
    return self._g.Request('DELETE', img_id)


class FipManager(BaseManager):
  """Handles requests on fips."""

  _type = 'floating_ips'

  def Create(self, resource_group, target, **kwargs) -> Dict[str, Any]:
    """Construct and send a vm create request.

    Args:
       resource_group: optional.
       target: id of the vm network interface.
       **kwargs:
         name - name of the fip.
         zone - zone name.

    Returns:
      returns floating IP addresses.
    """
    req_data = {'target': {'id': target}}
    if resource_group:
      req_data['resource_group'] = {'id': resource_group}
    if kwargs.get('name', None):
      req_data['name'] = kwargs.get('name', None)
    if kwargs.get('zone', None):
      req_data['zone'] = {'name': kwargs.get('name', None)}

    return self._g.create_floating_ips(req_data)

  def List(self) -> Dict[str, Any]:
    """Send a fip list request."""
    return self._g.Request('GET', self._type)

  def Show(self, fip) -> Dict[str, Any]:
    """Send a fip get request."""
    fip_id = self.GetUris(fip)
    return self._g.Request('GET', fip_id)

  def Delete(self, fip):
    """Send a fip delete request."""
    fip_id = self.GetUris(fip)
    return self._g.Request('DELETE', fip_id)
