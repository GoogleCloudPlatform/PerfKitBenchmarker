# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
"""Utilities for working with IBM Cloud resources."""

import threading
import time
import logging
import json
from collections import OrderedDict

from perfkitbenchmarker import flags
from perfkitbenchmarker.providers.ibmcloud import ibmcloud

FLAGS = flags.FLAGS

AVAILABLE = 'available'
RUNNING = 'running'
STOPPED = 'stopped'
DELIMITER = 'z-z'


class IbmAPICommand(object):
  """A IBM Cloud rest api command.

  Attributes:
    args: list of strings. Positional args to pass, typically
        specifying an operation to perform (e.g. ['image', 'list'] to list
        available images).
    flags: OrderedDict mapping flag name string to flag value. Flags to pass to
        ibm cloud rest api. If a provided value is True, the flag is passed without a value. 
        If a provided value is a list, the flag is passed multiple
        times, once with each value in the list.
  """

  gen = None
  gen_instmgr = None
  gen_imgmgr = None
  gen_keymgr = None
  gen_subnetmgr = None
  gen_fipmgr = None
  gen_volumemgr = None
  gen_vpcmgr = None
  gen_sgmgr = None
  ibmcloud_account_id = None
  ibmcloud_apikey = None
  ibmcloud_auth_token = None
  ibmcloud_auth_token_time = 0
  _lock = threading.Lock()
  base_time = 0.0
  num_requests = None
  debug = False

  def __init__(self, resource, *args):
    """Initializes a IbmAPICommand with the provided args and common flags.

    Args:
      resource: A Genesis resource of type BaseResource.
      *args: sequence of strings. Positional args to pass to genesis cli,
      typically specifying an operation to perform (e.g. ['image', 'list']
      to list available images).
    """
    if args and args[0] == 'login':
      IbmAPICommand.gen = ibmcloud.IbmCloud(account=self.ibmcloud_account_id,
                                            apikey=self.ibmcloud_apikey,
                                            verbose=self.debug,
                                            version='v1',
                                            silent=True,
                                            force=False)
      IbmAPICommand.gen_instmgr = ibmcloud.InstanceManager(IbmAPICommand.gen)
      IbmAPICommand.gen_imgmgr = ibmcloud.ImageManager(IbmAPICommand.gen)
      IbmAPICommand.gen_fipmgr = ibmcloud.FipManager(IbmAPICommand.gen)
      IbmAPICommand.gen_keymgr = ibmcloud.KeyManager(IbmAPICommand.gen)
      IbmAPICommand.gen_subnetmgr = ibmcloud.SubnetManager(IbmAPICommand.gen)
      IbmAPICommand.gen_volumemgr = ibmcloud.VolumeManager(IbmAPICommand.gen)
      IbmAPICommand.gen_vpcmgr = ibmcloud.VPCManager(IbmAPICommand.gen)
      IbmAPICommand.gen_sgmgr = ibmcloud.SGManager(IbmAPICommand.gen)
    self.args = list(args)
    self.flags = {}
    self.user_data = None
    self.sgid = None

  def GetToken(self):
    """Returns user token """
    return self.gen.GetToken()

  def CreateSgRules(self):
    """Creates some default security group rules """
    sgid = self.flags['sgid']
    self.gen_sgmgr.CreateRule(sgid, 'inbound', 'ipv4', '0.0.0.0/0', 'icmp', None)
    self.gen_sgmgr.CreateRule(sgid, 'inbound', 'ipv4', '0.0.0.0/0', 'tcp', 22)
    self.gen_sgmgr.CreateRule(sgid, 'inbound', 'ipv4', '0.0.0.0/0', 'tcp', None,
                              port_min=1024, port_max=50000)
    self.gen_sgmgr.CreateRule(sgid, 'inbound', 'ipv4', '0.0.0.0/0', 'udp', None,
                              port_min=1024, port_max=50000)
    self.gen_sgmgr.CreateRule(sgid, 'inbound', 'ipv4', '0.0.0.0/0', 'tcp', 443)

  def GetSgId(self):
    """returns sg id """
    logging.info('Looking up existing default sg')
    prefix = self.flags['prefix']
    resp = self.gen_sgmgr.List()
    if resp and 'security_groups' in resp:
      for item in resp['security_groups']:
        if item['vpc']['name'].startswith(prefix):  # find the one with matching vpc name
          return item['id']
    else:
      logging.error('Failed to retrieve security group id: %s', resp)
    return None

  def CreatePrefix(self):
    """Creates address prefix on vpc """
    return self.gen_vpcmgr.CreatePrefix(self.flags['vpcid'], self.flags['zone'],
                                        self.flags['cidr'], name=self.flags['name'])

  def CreateSubnet(self):
    """Creates subnet on vpc """
    kwargs = {}
    kwargs['name'] = self.flags['name']
    kwargs['zone'] = self.flags['zone']
    return self.gen_subnetmgr.Create(self.flags['cidr'], self.flags['vpcid'], **kwargs)

  def DeleteResource(self):
    """Deletes a resource """
    items = self.flags['items']
    data_mgr = self.gen_subnetmgr
    if items == 'vpcs':
      data_mgr = self.gen_vpcmgr
    elif items == 'keys':
      data_mgr = self.gen_keymgr
    resp = data_mgr.List()
    if resp and items in resp:
      for item in resp[items]:
        item_id = item.get('id')
        if item_id and item_id == self.flags['id']:
          data_mgr.Delete(item_id)
          logging.info('Deleted %s, id: %s', items, item_id)
    else:
      logging.info('No items found to delete: %s', items)

  def ListResources(self):
    """Returns a list of resource id's """
    items = self.flags['items']
    prefix = self.flags['prefix']
    zone = self.flags['zone']
    logging.info('Looking up existing %s matching prefix: %s, zone: %s', items, prefix, zone)
    data_mgr = self.gen_subnetmgr
    if items == 'vpcs':
      data_mgr = self.gen_vpcmgr
    elif items == 'keys':
      data_mgr = self.gen_keymgr
    resourceid = None  # instead of returning a list, we just return first matching item
    counter = 1
    while counter < 4:
      resp = data_mgr.List()
      if resp and items in resp:
        for item in resp[items]:
          itemid = item.get('id')
          name = item.get('name')
          if itemid and name and name.startswith(prefix):
            if items == 'subnets':
              if zone == item.get('zone')['name']:
                resourceid = itemid  # find the id that matches the zone name
                break
            else:  # for vpc and key
              resourceid = itemid  # # for vpcs, just get the first matching
              break
        break  # empty
      else:
        logging.error('Failed to retrieve %s, no data returned', items)
        counter += 1
        time.sleep(10)  # 5 still fails when accounts >= 10
    return resourceid

  def ListSubnetsExtra(self):
    """returns a list of extra subnets """
    items = 'subnets'
    prefix = self.flags['prefix']
    zone = self.flags['zone']
    logging.info('Looking up extra subnets for zone: %s', zone)
    data_mgr = self.gen_subnetmgr
    subnets = {}
    counter = 1
    while counter < 4:
      resp = data_mgr.List()
      if resp and items in resp:
        for item in resp[items]:
          itemid = item.get('id')
          name = item.get('name')
          ipv4_cidr_block = item.get('ipv4_cidr_block')
          if itemid and name and name.startswith(prefix):  # extra subnets start with 'sxs'
            if zone == item.get('zone')['name']:
              logging.info('Found extra subnet, name: %s, id: %s', name, itemid)
              names = name.split(DELIMITER)
              if names[0] not in subnets:
                subnets[names[0]] = {}
              subnets[names[0]]['id'] = itemid
              subnets[names[0]]['ipv4_cidr_block'] = ipv4_cidr_block  # needed to create route later
        break  # empty or done
      else:
        logging.error('Failed to retrieve extra subnets, no data returned')
        counter += 1
        time.sleep(10)  # 5 still fails when accounts >= 10
    return subnets

  def CreateVolume(self):
    """Creates an external disk volume """
    kwargs = {}
    kwargs['name'] = self.flags['name']
    kwargs['capacity'] = self.flags['capacity']
    kwargs['iops'] = self.flags['iops']
    kwargs['profile'] = self.flags['profile']
    if 'encryption_key' in self.flags:
      kwargs['encryption_key'] = self.flags['encryption_key']
    if 'resource_group' in self.flags:
      kwargs['resource_group'] = self.flags['resource_group']
    return json.dumps(self.gen_volumemgr.Create(self.flags['zone'], **kwargs))

  def DeleteVolume(self):
    """Deletes volume """
    return self.gen_volumemgr.Delete(self.flags['volume'])

  def ShowVolume(self):
    """Shows volume """
    return json.dumps(self.gen_volumemgr.Show(self.flags['volume']), indent=2)

  def CreateVpc(self):
    """Creastes a vpc """
    vpcid = None
    resp = self.gen_vpcmgr.Create(None, self.flags['name'])
    if resp:
      vpcid = resp.get('id')
      self.sgid = resp.get('default_security_group')['id']
      logging.info('Created vpc: %s, sgid: %s', vpcid, self.sgid)
      self.flags['sgid'] = self.sgid
      self.CreateSgRules()
      logging.info('Created sg rules for icmp, tcp 22/443, tcp/udp 1024-50000')
    return vpcid

  def CreateKey(self):
    """Creastes a ssh key in RIAS """
    kwargs = {}
    kwargs['name'] = self.flags['name']
    resp = self.gen_keymgr.Create(self.flags['pubkey'], 'rsa', **kwargs)
    return resp.get('id') if resp else None

  def CreateInstance(self):
    """Creastes a vm """
    kwargs = {}
    for arg in ['key', 'zone', 'subnet', 'networks', 'encryption_key', 'iops',
                'capacity', 'resource_group']:
      if arg in self.flags:
        kwargs[arg] = self.flags[arg]
    return json.dumps(self.gen_instmgr.Create(
      self.flags['name'],
      self.flags['imageid'],
      self.flags['profile'],
      self.flags['vpcid'],
      user_data=self.user_data,
      **kwargs
      ))

  def InstanceGetPrimaryVnic(self):
    """Creastes a primary network vnic on vm """
    resp = self.gen_instmgr.Show(self.flags['instanceid'])
    if 'primary_network_interface' in resp and 'href' in resp['primary_network_interface']:
      items = resp['primary_network_interface']['href'].split('/')
      logging.info('vnic found: %s', items[len(items) - 1])
      return items[len(items) - 1]
    logging.error('no href in instance: %s', self.flags['instanceid'])
    return None

  def InstanceFipCreate(self):
    """Creastes a FIP on vm """
    kwargs = {}
    kwargs['name'] = self.flags['name']
    return json.dumps(self.gen_fipmgr.Create(
      None,
      self.flags['target'],
      **kwargs
    ))

  def InstanceFipDelete(self):
    """Deletes FIP on vm """
    self.gen_fipmgr.Delete(self.flags['fip_id'])

  def _GetStatus(self, resp):
    """Gets instance status in resp """
    status = None
    if not resp:
      logging.error('http response is None')
      return status
    if 'status' in resp:
      status = resp['status']
    else:
      logging.error('No status found in resp: %s', resp)
    return status

  def InstanceStatus(self):
    """Gets instance status """
    resp = self.gen_instmgr.Show(self.flags['instanceid'])
    status = self._GetStatus(resp)
    logging.info('Instance status: %s, %s', self.flags['instanceid'], status)
    if status != RUNNING:
      endtime = time.time() + FLAGS.ibmcloud_timeout
      status = self._GetStatus(self.gen_instmgr.Show(self.flags['instanceid']))
      while status != RUNNING and time.time() < endtime:
        time.sleep(FLAGS.ibmcloud_polling_delay)
        status = self._GetStatus(self.gen_instmgr.Show(self.flags['instanceid']))
        logging.info('Checking instance status: %s, %s', self.flags['instanceid'], status)
    return status

  def InstanceStart(self):
    """Starts instance """
    resp = self.gen_instmgr.Start(self.flags['instanceid'])
    status = self._GetStatus(resp)
    logging.info('Instance start issued: %s, %s', self.flags['instanceid'], status)
    if status != RUNNING:
      endtime = time.time() + FLAGS.ibmcloud_timeout
      status = self._GetStatus(self.gen_instmgr.Show(self.flags['instanceid']))
      while status != RUNNING and time.time() < endtime:
        time.sleep(FLAGS.ibmcloud_polling_delay)
        status = self._GetStatus(self.gen_instmgr.Show(self.flags['instanceid']))
        logging.info('Checking instance start status: %s, %s', self.flags['instanceid'], status)
    return status

  def InstanceStop(self):
    """Stops instance """
    resp = self.gen_instmgr.Stop(self.flags['instanceid'], force=False)
    logging.info('Instance stop issued: %s', self.flags['instanceid'])
    resp = self.gen_instmgr.Stop(self.flags['instanceid'], force=True)  # force stop again
    status = self._GetStatus(resp)
    logging.info('Instance stop force issued: %s, %s', self.flags['instanceid'], status)
    if status != STOPPED:
      endtime = time.time() + FLAGS.ibmcloud_timeout
      status = self._GetStatus(self.gen_instmgr.Show(self.flags['instanceid']))
      while status != STOPPED and time.time() < endtime:
        time.sleep(FLAGS.ibmcloud_polling_delay * 2)
        status = self._GetStatus(self.gen_instmgr.Show(self.flags['instanceid']))
        logging.info('Checking instance stop status: %s, %s', self.flags['instanceid'], status)
    return status

  def InstanceDelete(self):
    """Deletes instance """
    self.gen_instmgr.Delete(self.flags['instanceid'])

  def InstanceList(self):
    """Lists instances """
    return self.gen_instmgr.List()

  def InstanceShow(self):
    """GETs the instance """
    return self.gen_instmgr.Show(self.flags['instanceid'])

  def InstanceInitializationShow(self):
      return self.gen_instmgr.ShowInitialization(self.flags['instanceid'])

  def InstanceVnicCreate(self):
    """Creates instance vnic """
    ip_addr = None
    resp = self.gen_instmgr.CreateVnic(self.flags['instanceid'], self.flags['name'], self.flags['subnet'])
    status = self._GetStatus(resp)
    vnicid = resp['id']
    port_speed = resp['port_speed']
    logging.info('Vnic created, vnicid: %s, port speed: %s, vnic status: %s', vnicid, port_speed, status)
    if status != AVAILABLE:
      endtime = time.time() + FLAGS.ibmcloud_timeout
      resp = self.gen_instmgr.ShowVnic(self.flags['instanceid'], vnicid)
      status = self._GetStatus(resp)
      while status != AVAILABLE and time.time() < endtime:
        time.sleep(FLAGS.ibmcloud_polling_delay)
        resp = self.gen_instmgr.ShowVnic(self.flags['instanceid'], vnicid)
        status = self._GetStatus(resp)
        logging.info('Checking instance vnic status: %s', status)
    if status == AVAILABLE:
      ip_addr = resp.get('primary_ipv4_address')
      logging.info('primary_ipv4_address: %s', ip_addr)
    assert status == AVAILABLE
    return vnicid, ip_addr

  def InstanceVnicShow(self):
    """GETs instance vnic """
    return json.dumps(self.gen_instmgr.ShowVnic(
      self.flags['instance'],
      self.flags['vnicid']
      ), indent=2)

  def InstanceVnicList(self):
    """GETs all vnics on instance """
    return json.dumps(self.gen_instmgr.ListVnics(
      self.flags['instance']
      ), indent=2)

  def InstanceCreateVolume(self):
    """Attaches a volume to instance """
    return json.dumps(self.gen_instmgr.CreateVolume(
      self.flags['instance'],
      self.flags['name'],
      self.flags['volume'],
      True
      ), indent=2)

  def InstanceDeleteVolume(self):
    """Deletes volume on instance """
    return json.dumps(self.gen_instmgr.DeleteVolume(
      self.flags['instance'],
      self.flags['volume']
      ), indent=2)

  def InstanceShowVolume(self):
    """GETs instance volumes """
    for item in self.gen_instmgr.ListVolumes(self.flags['instance'])['volume_attachments']:
      if item['volume'].get('id') == self.flags['volume']:
        return json.dumps(self.gen_instmgr.ShowVolume(
          self.flags['instance'],
          item.get('id')
          ), indent=2)

  def ImageList(self):
    """Lists images """
    return self.gen_imgmgr.List(account=self.ibmcloud_account_id)

  def GetImageId(self):
    """GETs image id that matches the name """
    resp = self.gen_imgmgr.List()['images']
    if resp is not None:
      for image in resp:
        if image['name'].startswith(self.flags['image_name']) and image['status'] == AVAILABLE:
          return image['id']
    return None

  def ImageShow(self):
    """GETs image by id """
    return self.gen_imgmgr.Show(self.flags['imageid'])

