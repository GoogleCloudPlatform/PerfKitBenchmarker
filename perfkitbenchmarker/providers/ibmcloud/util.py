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

import os
import json
import yaml

from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.ibmcloud import ibmcloud

WINDOWS = 'windows'  # all windows
UBUNTU = 'ubuntu'  # all ubuntu
REDHAT = 'redhat'  # all redhat
DEBIAN = 'debian'  # all debian
CENTOS = 'centos'  # all centos
UNKNOWN = 'unknown'

SUBNET_SUFFIX = 's'
DELIMITER = 'z-z'

# these are used to create extra subnets for multi vnic support in perfkit,
# not used for vm provisioning, up to 4 extra totaling 5 subnets
SUBNET_SUFFIX_EXTRA = 'sxs'
SUBNETX1 = SUBNET_SUFFIX_EXTRA + '1'
SUBNETX2 = SUBNET_SUFFIX_EXTRA + '2'
SUBNETX3 = SUBNET_SUFFIX_EXTRA + '3'
SUBNETX4 = SUBNET_SUFFIX_EXTRA + '4'
SUBNETXS = [SUBNETX1, SUBNETX2, SUBNETX3, SUBNETX4]

SUBNETS_EXTRA = {
    SUBNETX1: ['10.101.20.0/24', '10.102.20.0/24', '10.103.20.0/24', '10.104.20.0/24', '10.105.20.0/24'],
    SUBNETX2: ['10.101.30.0/24', '10.102.30.0/24', '10.103.30.0/24', '10.104.30.0/24', '10.105.30.0/24'],
    SUBNETX3: ['10.101.40.0/24', '10.102.40.0/24', '10.103.40.0/24', '10.104.40.0/24', '10.105.40.0/24'],
    SUBNETX4: ['10.101.50.0/24', '10.102.50.0/24', '10.103.50.0/24', '10.104.50.0/24', '10.105.50.0/24']
}

SUBNETS_EXTRA_GATEWAY = {
    SUBNETX1: ['10.101.20.1', '10.102.20.1', '10.103.20.1', '10.104.20.1', '10.105.20.1'],
    SUBNETX2: ['10.101.30.1', '10.102.30.1', '10.103.30.1', '10.104.30.1', '10.105.30.1'],
    SUBNETX3: ['10.101.40.1', '10.102.40.1', '10.103.40.1', '10.104.40.1', '10.105.40.1'],
    SUBNETX4: ['10.101.50.1', '10.102.50.1', '10.103.50.1', '10.104.50.1', '10.105.50.1']
}


def ReadConfig(config):
  """Reads in config yml

  Args:
    config: config file name.

  Returns:
    The json representation of the config file.
  """
  try:
    with open(config, 'r') as stream:
      data = json.dumps(yaml.load(stream), sort_keys=True)
      return json.loads(data)
  except Exception as ex:
    raise errors.Error('Failed to load configuration file %s, %s', config, ex)


def GetSubnetIndex(ipv4_cidr_block):
  """Finds the index for the given cidr

  Args:
    ipv4_cidr_block: cidr to find.

  Returns:
    The index number of the found cidr as in the predefined list
  """
  for name in SUBNETXS:
    ip_list = SUBNETS_EXTRA[name]
    for i in range(len(ip_list)):
      if ip_list[i] == ipv4_cidr_block:
        return i
  return -1


def GetRouteCommands(data, index, target_index):
  """Creates a list of ip route commands in text format to run on vm,
    not used on normal perfkit runs.

  Args:
    data: output from route command on vm.
    index: subnet index.
    target_index: target subnet index.

  Returns:
    The index number of the found cidr as in the predefined list
  """
  route_cmds = []
  if data:
    for subnet_name in SUBNETXS:
      subnet_cidr_block = SUBNETS_EXTRA[subnet_name][index]
      target_cidr_block = SUBNETS_EXTRA[subnet_name][target_index]
      subnet_gateway = SUBNETS_EXTRA_GATEWAY[subnet_name][index]
      route_entry = subnet_cidr_block.split('/24')
      interface = None
      for line in data.splitlines():
        items = line.split()
        if len(items) > 6 and items[0] == route_entry[0]:
          interface = items[7]
          route_cmds.append('ip route add ' + target_cidr_block + ' via ' + \
                            subnet_gateway + ' dev ' + interface)
  return route_cmds


def GetMinMax(data):
  """Finds min and max values in the given list of values"""
  if not data:
    return 0.0, 0.0
  min_value = float(data[0])
  max_value = float(data[0])
  for item in data:
    value = float(item)
    if value > max_value:
      max_value = value
    if value < min_value:
      min_value = value
  return min_value, max_value


def GetBaseOs(osdata):
  """Finds the base os name to use

  Args:
    osdata: json representation of os information of an image.

  Returns:
    Short name of the os name
  """
  if 'name' in osdata and osdata['name'] is not None:
    osname = osdata['name'].lower()
    if 'window' in osname:
      return WINDOWS
    elif 'red' in osname or 'rhel' in osname:
      return REDHAT
    elif 'centos' in osname:
      return CENTOS
    elif 'debian' in osname:
      return DEBIAN
    elif 'ubuntu' in osname:
      return UBUNTU
  return UNKNOWN


def GetGen(account):
  """Creates a ibmcloud access object """
  gen = ibmcloud.IbmCloud(account=account[0], apikey=account[1], verbose=False, \
                     version='v1', silent=True, force=True)
  if not gen.token():
    gen.set_token()  # one more try
  return gen


def GetImageId(account, imgname):
  """Returns image id matching the image name """
  data_mgr = ibmcloud.ImageManager(GetGen(account))
  resp = data_mgr.List()['images']
  if resp is not None:
    for image in resp:
      if image['name'] == imgname:
        return image['id']
  return None


def GetImageIdInfo(account, imageid):
  """Returns OS information matching the image id """
  data_mgr = ibmcloud.ImageManager(GetGen(account))
  return GetOsInfo(data_mgr.Show(imageid))


def GetOsInfo(image):
  """Returns os information in json format

  Args:
    image: json representation of os information of an image.

  Returns:
    OS details of the image in json format
  """
  data = {}
  custom = False
  if image and 'operating_system' in image:
    data = image['operating_system']
    if 'href' in data:
      del data['href']  # delete this, does not seem necessary
    if 'custom' in image['name']:  # this name is not the name in operating_system
      custom = True
  else:
    # if lookup failed, try read from env if any is set
    data['version'] = os.environ.get('RIAS_IMAGE_OS_VERSION')
    data['vendor'] = os.environ.get('RIAS_IMAGE_OS_VENDOR')
    data['name'] = os.environ.get('IMGNAME')
    if data['name'] is not None and 'custom' in data['name']:
      custom = True
    data['architecture'] = os.environ.get('RIAS_IMAGE_OS_ARCH')
  data['base_os'] = GetBaseOs(data)
  data['custom'] = custom
  return data
