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
"""Utilities for working with IBM Cloud resources."""

import json
import logging
import os
import threading
import time

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.ibmcloud import ibmcloud_manager
from perfkitbenchmarker.providers.ibmcloud import util

FLAGS = flags.FLAGS

ONE_HOUR = 3600


class Enum(tuple):
  __getattr__ = tuple.index


class States(Enum):
  AVAILABLE = 'available'
  RUNNING = 'running'
  STOPPED = 'stopped'
  ATTACHED = 'attached'


class IbmAPICommand(object):
  """A IBM Cloud rest api command.

  Attributes:
    args: list of strings. Positional args to pass, typically specifying an
      operation to perform (e.g. ['image', 'list'] to list available images).
    flags: a dict mapping flag name string to flag value. Flags to pass to ibm
      cloud rest api. If a provided value is True, the flag is passed without a
      value. If a provided value is a list, the flag is passed multiple times,
      once with each value in the list.
    ibmcloud_auth_token: Auth token for cmd.
    ibmcloud_auth_token_time: Time when auth token was acquired.
    user_data: user data info.
    vpc_id: vpc_id - passed in flags.
    sg_id: sg_id - passed in flags.
    zone: zone - passed in flags.
    cidr: cidr - passed in flags.
    name: name - passed in flags.
    prefix: prefix - passed in flags.
    items: items - passed in flags
    capacity: capacity - passed in flags.
    iops: iops - passed in flags.
    profile: profile - passed in flags.
    encryption_key: encryption_key - passed in flags.
    resource_group: resource_group - passed in flags.
    volume: volume - passed in flags.
    pubkey: pubkey - passed in flags.
    imageid: imageid - passed in flags.
    target: target - passed in flags.
    vnic_id: vnic_id - passed in flags.
    fip_id: fip_id - passed in flags.
    instance_id: instance_id - passed in flags.
    subnet: subnet - passed in flags.
    image_id: image_id - passed in flags.
    image_name: image_name - passed in flags.
    resource_id: resource_id - passed in flags.
    data_encryption_key: data encryption key for account.
    boot_encryption_key: boot encryption key for account.
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

  def __init__(self, *args, **kwargs):
    """Initializes a IbmAPICommand with the provided args and flags.

    Args:
      *args: sequence of strings. Positional args to pass to rest api calls,
        typically specifying an operation to perform (e.g. ['image', 'list'] to
        list available images).
      **kwargs: resource - A IBM Cloud resource of type BaseResource.
    """
    self._CheckEnvironment()
    if (self.gen is None or
        time.time() - self.ibmcloud_auth_token_time >= ONE_HOUR):
      IbmAPICommand.gen = ibmcloud_manager.IbmCloud(
          account=self.ibmcloud_account_id,
          apikey=self.ibmcloud_apikey,
          verbose=False,
          version='v1',
          silent=True,
          force=False)
      IbmAPICommand.gen_instmgr = ibmcloud_manager.InstanceManager(
          IbmAPICommand.gen)
      IbmAPICommand.gen_imgmgr = ibmcloud_manager.ImageManager(
          IbmAPICommand.gen)
      IbmAPICommand.gen_fipmgr = ibmcloud_manager.FipManager(IbmAPICommand.gen)
      IbmAPICommand.gen_keymgr = ibmcloud_manager.KeyManager(IbmAPICommand.gen)
      IbmAPICommand.gen_subnetmgr = ibmcloud_manager.SubnetManager(
          IbmAPICommand.gen)
      IbmAPICommand.gen_volumemgr = ibmcloud_manager.VolumeManager(
          IbmAPICommand.gen)
      IbmAPICommand.gen_vpcmgr = ibmcloud_manager.VPCManager(IbmAPICommand.gen)
      IbmAPICommand.gen_sgmgr = ibmcloud_manager.SGManager(IbmAPICommand.gen)
      self.ibmcloud_auth_token = self.GetToken()
      self.ibmcloud_auth_token_time = time.time()

    self.args = args
    self.flags = kwargs
    self.user_data = None


#     self.sgid = None

  @property
  def vpc_id(self):
    return self.flags['vpcid']

  @property
  def sg_id(self):
    return self.flags['sgid']

  @property
  def zone(self):
    return self.flags['zone']

  @property
  def cidr(self):
    return self.flags['cidr']

  @property
  def name(self):
    return self.flags['name']

  @property
  def prefix(self):
    return self.flags['prefix']

  @property
  def items(self):
    return self.flags['items']

  @property
  def capacity(self):
    return self.flags['capacity']

  @property
  def iops(self):
    return self.flags['iops']

  @property
  def profile(self):
    return self.flags['profile']

  @property
  def encryption_key(self):
    return self.flags['encryption_key']

  @property
  def resource_group(self):
    return self.flags['resource_group']

  @property
  def volume(self):
    return self.flags['volume']

  @property
  def pubkey(self):
    return self.flags['pubkey']

  @property
  def image_id(self):
    return self.flags['imageid']

  @property
  def target(self):
    return self.flags['target']

  @property
  def vnic_id(self):
    return self.flags['vnicid']

  @property
  def fip_id(self):
    return self.flags['fipid']

  @property
  def instance_id(self):
    return self.flags['instanceid']

  @property
  def subnet(self):
    return self.flags['subnet']

  @property
  def image_name(self):
    return self.flags['image_name']

  @property
  def resource_id(self):
    return self.flags['id']

  def _CheckEnvironment(self):
    """Get the information needed to authenticate on IBM Cloud.

    Check for the config if env variables for apikey is not set.
    """
    if not os.environ.get('IBMCLOUD_ENDPOINT'):
      raise errors.Config.InvalidValue(
          'PerfKit Benchmarker on IBM Cloud requires that the '
          'environment variable IBMCLOUD_ENDPOINT is set.')
    account = util.Account(
        os.environ.get('IBMCLOUD_ACCOUNT_ID'),
        os.environ.get('IBMCLOUD_APIKEY'), None)
    if not account.name or not account.apikey:
      accounts = os.environ.get('IBMCLOUD_ACCOUNTS')  # read from config file
      if accounts and accounts:
        config = util.ReadConfig(accounts)
        benchmark = config[FLAGS.benchmarks[0]][0]
        # first account
        account = util.Account(benchmark[0], benchmark[1], benchmark[2])
        logging.info('account_id: %s', account.name)
        if FLAGS.ibmcloud_datavol_encryption_key:  # if this flag is set
          self.data_encryption_key = account.enckey
          logging.info('KP key to use, data: %s', self.data_encryption_key)
        if FLAGS.ibmcloud_bootvol_encryption_key:  # use same key as data
          self.boot_encryption_key = account.enckey
          logging.info('KP key to use, boot: %s', self.boot_encryption_key)

    if not account.name or not account.apikey:
      raise errors.Config.InvalidValue(
          'PerfKit Benchmarker on IBM Cloud requires that the '
          'environment variables IBMCLOUD_ACCOUNT_ID and IBMCLOUD_APIKEY '
          'are correctly set.')

  def GetToken(self):
    """Returns user token."""
    return self.gen.GetToken()

  def CreateSgRules(self):
    """Creates default security group rules needed for vms to communicate."""
    self.gen_sgmgr.CreateRule(self.sg_id, 'inbound', 'ipv4', '0.0.0.0/0',
                              'icmp', None)
    self.gen_sgmgr.CreateRule(self.sg_id, 'inbound', 'ipv4', '0.0.0.0/0', 'tcp',
                              22)
    self.gen_sgmgr.CreateRule(
        self.sg_id,
        'inbound',
        'ipv4',
        '0.0.0.0/0',
        'tcp',
        None,
        port_min=1024,
        port_max=50000)
    self.gen_sgmgr.CreateRule(
        self.sg_id,
        'inbound',
        'ipv4',
        '0.0.0.0/0',
        'udp',
        None,
        port_min=1024,
        port_max=50000)
    self.gen_sgmgr.CreateRule(self.sg_id, 'inbound', 'ipv4', '0.0.0.0/0', 'tcp',
                              443)

  def GetSecurityGroupId(self):
    """Returns default security group id."""
    logging.info('Looking up existing default sg')
    prefix = self.prefix
    resp = self.gen_sgmgr.List()
    if resp and 'security_groups' in resp:
      for item in resp['security_groups']:
        # find the one with matching vpc name
        if item['vpc']['name'].startswith(prefix):
          return item['id']
    else:
      logging.error('Failed to retrieve security group id: %s', resp)
    return None

  def CreatePrefix(self):
    """Creates address prefix on vpc needed to attach subnets.

    Flags:
      vpcid: ibm cloud vpc id.
      zone: name of the zone within ibm network.
      cidr: ip range that this prefix will cover.
      name: name of this prefix.

    Returns:
      The json representation of the created address prefix.
    """
    return self.gen_vpcmgr.CreatePrefix(
        self.vpc_id, self.zone, self.cidr, name=self.name)

  def CreateSubnet(self):
    """Creates a subnet on the vpc.

    Flags:
      vpcid: ibm cloud vpc id.
      zone: name of the zone within ibm network.
      cidr: ip range for the subnet.
      name: name of this subnet.

    Returns:
      The json representation of the created subnet.
    """
    kwargs = {}
    kwargs['name'] = self.name
    kwargs['zone'] = self.zone
    return self.gen_subnetmgr.Create(self.cidr, self.vpc_id, **kwargs)

  def DeleteResource(self):
    """Deletes a resource based on items set in the flags.

    Flags:
      items: type of resource to delete.
      id: matching id to delete.

    """
    data_mgr = self.gen_subnetmgr
    if self.items == 'vpcs':
      data_mgr = self.gen_vpcmgr
    elif self.items == 'keys':
      data_mgr = self.gen_keymgr
    resp = data_mgr.List()
    if resp and self.items in resp:
      for item in resp[self.items]:
        item_id = item.get('id')
        if item_id and item_id == self.resource_id:
          try:
            data_mgr.Delete(item_id)
            logging.info('Deleted %s, id: %s', self.items, item_id)
          except Exception:  # pylint: disable=broad-except
            pass
    else:
      logging.info('No items found to delete: %s', self.items)

  def GetResource(self):
    """Returns id of the found resource matching the resource type and prefix.

    Flags:
      items: type of resource.
      zone: name of the zone within ibm network, for subnets.
      preifx: a label matching the resource name.

    Returns:
      The id of the first matching resource.
    """
    logging.info('Looking up existing %s matching prefix: %s, zone: %s',
                 self.items, self.prefix, self.zone)
    data_mgr = self.gen_subnetmgr
    if self.items == 'vpcs':
      data_mgr = self.gen_vpcmgr
    elif self.items == 'keys':
      data_mgr = self.gen_keymgr
    # instead of returning a list, we just return first matching item
    resourceid = None
    resp = data_mgr.List()
    if resp and self.items in resp:
      for item in resp[self.items]:
        itemid = item.get('id')
        name = item.get('name')
        if itemid and name and name.startswith(self.prefix):
          if self.items == 'subnets':
            if self.zone == item.get('zone')['name']:
              resourceid = itemid  # find the id that matches the zone name
              break
          else:  # for vpc and key
            resourceid = itemid  # # for vpcs, just get the first matching
            break
      logging.info('Resource found, id: %s', resourceid)
    else:
      logging.error('Failed to retrieve %s, no data returned', self.items)
    return resourceid

  def ListSubnetsExtra(self):
    """Returns a list of extra subnets, this is not used for regular runs.

    Flags:
      zone: name of the zone within ibm network, for subnets.
      preifx: a label matching the resource name.

    Returns:
      List of ids of the additional subnets matching the prefix and zone.
    """
    items = 'subnets'
    logging.info('Looking up extra subnets for zone: %s', self.zone)
    data_mgr = self.gen_subnetmgr
    subnets = {}
    resp = data_mgr.List()
    if resp and items in resp:
      for item in resp[items]:
        itemid = item.get('id')
        name = item.get('name')
        ipv4_cidr_block = item.get('ipv4_cidr_block')
        # extra subnets start with 'sxs'
        if itemid and name and name.startswith(self.prefix):
          if self.zone == item.get('zone')['name']:
            logging.info('Found extra subnet, name: %s, id: %s', name, itemid)
            names = name.split(util.DELIMITER)
            if names[0] not in subnets:
              subnets[names[0]] = {}
            subnets[names[0]]['id'] = itemid
            # needed to create route later
            subnets[names[0]]['ipv4_cidr_block'] = ipv4_cidr_block
    else:
      logging.error('Failed to retrieve extra subnets, no data returned')
    return subnets

  def CreateVolume(self):
    """Creates an external disk volume.

    Flags:
      name: name of the new volume.
      capacity: size of the volume in GB.
      iops: desired iops on the volume.
      profile: profile name of the volume, usually custom for large volumes
      encryption_key: optional, encrytion key
      resource_group: optional, resource group id to assign the volume to.

    Returns:
      json representation of the created volume.
    """
    kwargs = {}
    kwargs['name'] = self.name
    kwargs['capacity'] = self.capacity
    kwargs['iops'] = self.iops
    kwargs['profile'] = self.profile
    if 'encryption_key' in self.flags:
      kwargs['encryption_key'] = self.encryption_key
    if 'resource_group' in self.flags:
      kwargs['resource_group'] = self.resource_group
    return json.dumps(self.gen_volumemgr.Create(self.zone, **kwargs))

  def DeleteVolume(self):
    """Deletes volume."""
    return self.gen_volumemgr.Delete(self.volume)

  def ShowVolume(self):
    """Shows volume in json."""
    return json.dumps(self.gen_volumemgr.Show(self.volume), indent=2)

  def CreateVpc(self):
    """Creates a vpc.

    Flags:
      name: name of the vpc to create.

    Returns:
      The id of the created vpc.
    """
    vpcid = None
    resp = self.gen_vpcmgr.Create(self.name)
    if resp:
      vpcid = resp.get('id')
      sgid = resp.get('default_security_group')['id']
      logging.info('Created vpc: %s, sgid: %s', vpcid, sgid)
      self.flags['sgid'] = sgid
      self.CreateSgRules()
      logging.info('Created sg rules for icmp, tcp 22/443, tcp/udp 1024-50000')
    return vpcid

  def CreateKey(self):
    """Creates a ssh key in ibm cloud.

    Flags:
      name: name of the key to create.
      pubkey: public key to use.

    Returns:
      The id of the created ssh key.
    """
    kwargs = {}
    kwargs['name'] = self.flags['name']
    resp = self.gen_keymgr.Create(self.pubkey, 'rsa', **kwargs)
    return resp.get('id') if resp else None

  def CreateInstance(self):
    """Creates a vm.

    Flags:
      name: name of the vm.
      imageid: id of the image to use.
      profile: machine type for the vm.
      vpcid: id of the vpc.
      user_data: this is set if windows
      optional: optional args added to kwargs.

    Returns:
      The json representation of the created vm.
    """
    kwargs = {}
    for arg in [
        'key', 'zone', 'subnet', 'networks', 'encryption_key', 'iops',
        'capacity', 'resource_group'
    ]:
      if arg in self.flags:
        kwargs[arg] = self.flags[arg]
    return json.dumps(
        self.gen_instmgr.Create(
            self.name,
            self.image_id,
            self.profile,
            self.vpc_id,
            user_data=self.user_data,
            **kwargs))

  def InstanceGetPrimaryVnic(self):
    """Finds the primary network vnic on vm.

    Flags:
      instanceid: id of the vm.

    Returns:
      The found vnic id.
    """
    resp = self.gen_instmgr.Show(self.instance_id)
    if 'primary_network_interface' in resp and 'href' in resp[
        'primary_network_interface']:
      items = resp['primary_network_interface']['href'].split('/')
      logging.info('vnic found: %s', items[len(items) - 1])
      return items[len(items) - 1]
    logging.error('no href in instance: %s', self.instance_id)
    return None

  def InstanceFipCreate(self):
    """Creates a FIP on vm.

    Flags:
      name: name of the fip.

    Returns:
      json represenation of the created fip.
    """
    kwargs = {}
    kwargs['name'] = self.name
    return json.dumps(self.gen_fipmgr.Create(None, self.target, **kwargs))

  def InstanceFipDelete(self):
    """Deletes FIP on vm matching the id."""
    self.gen_fipmgr.Delete(self.fip_id)

  def _GetStatus(self, resp):
    """Returns instance status in the given resp."""
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
    """Returns instance status matching the instance id."""
    resp = self.gen_instmgr.Show(self.instance_id)
    status = self._GetStatus(resp)
    logging.info('Instance status: %s, %s', self.instance_id, status)
    if status != States.RUNNING:
      endtime = time.time() + FLAGS.ibmcloud_timeout
      status = self._GetStatus(self.gen_instmgr.Show(self.instance_id))
      while status != States.RUNNING and time.time() < endtime:
        time.sleep(FLAGS.ibmcloud_polling_delay)
        status = self._GetStatus(self.gen_instmgr.Show(self.instance_id))
        logging.info('Checking instance status: %s, %s', self.instance_id,
                     status)
    return status

  def InstanceStart(self):
    """Starts the instance matching instance id and returns final status."""
    resp = self.gen_instmgr.Start(self.flags['instanceid'])
    status = self._GetStatus(resp)
    logging.info('Instance start issued: %s, %s', self.flags['instanceid'],
                 status)
    if status != States.RUNNING:
      endtime = time.time() + FLAGS.ibmcloud_timeout
      status = self._GetStatus(self.gen_instmgr.Show(self.flags['instanceid']))
      while status != States.RUNNING and time.time() < endtime:
        time.sleep(FLAGS.ibmcloud_polling_delay)
        status = self._GetStatus(
            self.gen_instmgr.Show(self.flags['instanceid']))
        logging.info('Checking instance start status: %s, %s',
                     self.flags['instanceid'], status)
    return status

  def InstanceStop(self):
    """Stops instance matching the instance id and returns final status."""
    resp = self.gen_instmgr.Stop(self.instance_id, force=False)
    logging.info('Instance stop issued: %s', self.instance_id)
    # force stop again
    resp = self.gen_instmgr.Stop(self.instance_id, force=True)
    status = self._GetStatus(resp)
    logging.info('Instance stop force issued: %s, %s', self.instance_id, status)
    if status != States.STOPPED:
      endtime = time.time() + FLAGS.ibmcloud_timeout
      status = self._GetStatus(self.gen_instmgr.Show(self.instance_id))
      while status != States.STOPPED and time.time() < endtime:
        time.sleep(FLAGS.ibmcloud_polling_delay * 2)
        status = self._GetStatus(self.gen_instmgr.Show(self.instance_id))
        logging.info('Checking instance stop status: %s, %s', self.instance_id,
                     status)
    return status

  def InstanceDelete(self):
    """Deletes instance matching the instance id."""
    self.gen_instmgr.Delete(self.instance_id)

  def InstanceList(self):
    """Lists instances and returns json objects of all found instances."""
    return self.gen_instmgr.List()

  def InstanceShow(self):
    """Returns the json respresentation of the matching instance id."""
    return self.gen_instmgr.Show(self.instance_id)

  def InstanceInitializationShow(self):
    """Returns the json format of the config used to initialize instance id."""
    return self.gen_instmgr.ShowInitialization(self.instance_id)

  def InstanceVnicCreate(self):
    """Creates a vnic on the instance.

    Flags:
      instanceid: id of the vm.
      name: name of the vnic.
      subnet: subnet id.

    Returns:
      id of the created vnic and assigned private ip address.
    """
    ip_addr = None
    resp = self.gen_instmgr.CreateVnic(self.instance_id, self.name, self.subnet)
    status = self._GetStatus(resp)
    vnicid = resp['id']
    port_speed = resp['port_speed']
    logging.info('Vnic created, vnicid: %s, port speed: %s, vnic status: %s',
                 vnicid, port_speed, status)
    if status != States.AVAILABLE:
      endtime = time.time() + FLAGS.ibmcloud_timeout
      resp = self.gen_instmgr.ShowVnic(self.instance_id, vnicid)
      status = self._GetStatus(resp)
      while status != States.AVAILABLE and time.time() < endtime:
        time.sleep(FLAGS.ibmcloud_polling_delay)
        resp = self.gen_instmgr.ShowVnic(self.instance_id, vnicid)
        status = self._GetStatus(resp)
        logging.info('Checking instance vnic status: %s', status)
    if status == States.AVAILABLE:
      ip_addr = resp.get('primary_ipv4_address')
      logging.info('primary_ipv4_address: %s', ip_addr)
    assert status == States.AVAILABLE
    return vnicid, ip_addr

  def InstanceVnicShow(self):
    """GETs instance vnic.

    Flags:
      instanceid: id of the vm.
      vnicid: id of the vnic.

    Returns:
      json representation of the found vnic.
    """
    return json.dumps(
        self.gen_instmgr.ShowVnic(self.instance_id, self.vnic_id), indent=2)

  def InstanceVnicList(self):
    """Returns a list of all vnics on instance.

    Flags:
      instanceid: id of the vm.

    Returns:
      json objects of all vnics on the instance.
    """
    return json.dumps(self.gen_instmgr.ListVnics(self.instance_id), indent=2)

  def InstanceCreateVolume(self):
    """Attaches a volume to instance.

    Flags:
      instanceid: id of the vm.
      name: name of the volume on the vm.
      volume: id of the volume to attach to the vm.
      True: always true, delete the volume when instance is deleted.

    Returns:
      json representation of the attached volume on the instance.
    """
    return json.dumps(
        self.gen_instmgr.CreateVolume(self.instance_id, self.name, self.volume,
                                      True),
        indent=2)

  def InstanceDeleteVolume(self):
    """Deletes the volume on the vm matching instanceid and volume."""
    return json.dumps(
        self.gen_instmgr.DeleteVolume(self.instance_id, self.volume), indent=2)

  def InstanceShowVolume(self):
    """Returns json representation of the volume matching instanceid."""
    for item in self.gen_instmgr.ListVolumes(
        self.instance_id)['volume_attachments']:
      if item['volume'].get('id') == self.volume:
        return json.dumps(
            self.gen_instmgr.ShowVolume(self.instance_id, item.get('id')),
            indent=2)

  def ImageList(self):
    """Returns json objects of all available images."""
    return self.gen_imgmgr.List(account=self.ibmcloud_account_id)

  def GetImageId(self):
    """Returns image id that matches the image name."""
    resp = self.gen_imgmgr.List()['images']
    if resp is not None:
      for image in resp:
        if image['name'].startswith(
            self.image_name) and image['status'] == States.AVAILABLE:
          return image['id']
    return None

  def ImageShow(self):
    """Returns json object of the image matching the id."""
    return self.gen_imgmgr.Show(self.image_id)
