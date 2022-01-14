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

import dataclasses
import os
from typing import Any, Dict, Optional

from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker.providers.ibmcloud import ibmcloud_manager
import yaml

WINDOWS = os_types.WINDOWS  # all windows
DEBIAN = os_types.DEBIAN  # all debian
UBUNTU = 'ubuntu'  # all ubuntu
REDHAT = 'redhat'  # all redhat
CENTOS = 'centos'  # all centos
UNKNOWN = 'unknown'

SUBNET_SUFFIX = 's'
DELIMITER = 'z-z'

# for windows
USER_DATA = (
    'Content-Type: text/x-shellscript; '
    "charset=\"us-ascii\"\nContent-Transfer-Encoding: "
    '7bit\nContent-Disposition: attachment; '
    "filename=\"set-content.ps1\"\n#ps1_sysnative\nfunction "
    'Setup-Remote-Desktop () {\nSet-ItemProperty '
    "\"HKLM:\\SYSTEM\\CurrentControlSet\\Control\\Terminal Server\" -Name "
    'fDenyTSConnections -Value 0\nSet-ItemProperty '
    "\"HKLM:\\SYSTEM\\CurrentControlSet\\Control\\Terminal "
    "Server\\WinStations\\RDP-Tcp\" -Name \"UserAuthentication\" -Value "
    "1\nEnable-NetFireWallRule -DisplayGroup \"Remote Desktop\"\n}\nfunction "
    "Setup-Ping () {\nSet-NetFirewallRule -DisplayName \"File and Printer "
    "Sharing (Echo Request - ICMPv4-In)\" -enabled True\nSet-NetFirewallRule "
    "-DisplayName \"File and Printer Sharing (Echo Request - ICMPv6-In)\" "
    '-enabled True\n}\nSetup-Remote-Desktop\nSetup-Ping\nNew-NetFirewallRule '
    "-DisplayName \"Allow winrm https 5986\" -Direction Inbound -Action Allow "
    '-Protocol TCP -LocalPort 5986\nwinrm set winrm/config/service/auth '
    "'@{Basic=\"true\";Certificate=\"true\"}'\n$cert=New-SelfSignedCertificate"
    ' -certstorelocation cert:\\localmachine\\my -dnsname '
    '*\n$thumb=($cert).Thumbprint\nNew-WSManInstance -ResourceURI '
    "winrm/config/Listener -SelectorSet @{Address=\"*\";Transport=\"HTTPS\"} "
    "-ValueSet @{CertificateThumbprint=\"$thumb\"}\npowercfg /SetActive "
    "(powercfg /List | %{if ($_.Contains(\"High "
    "performance\")){$_.Split()[3]}})\nSet-NetAdapterAdvancedProperty -Name "
    'Ethernet -RegistryKeyword MTU -RegistryValue 9000\n')


@dataclasses.dataclass(frozen=True)
class Account:
  name: Optional[str]
  apikey: Optional[str]
  enckey: Optional[str]


def ReadConfig(config: str) -> Dict[Any, Any]:
  """Reads in config yml.

  Args:
    config: config file name.

  Returns:
    The json representation of the config file.
  """
  try:
    with open(config, 'r') as stream:
      return yaml.safe_load(stream)
  except Exception as ex:
    raise errors.Error(('Failed to load configuration file %s, %s', config, ex))


def GetBaseOs(osdata: Dict[str, str]):
  """Finds the base os name to use.

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


def GetGen(account: Account):
  """Creates a ibmcloud access object."""
  gen = ibmcloud_manager.IbmCloud(
      account=account.name,
      apikey=account.apikey,
      verbose=False,
      version='v1',
      silent=True,
      force=True)
  if not gen.Token():
    gen.SetToken()  # one more try
  return gen


def GetImageId(account: Account, imgname: str):
  """Returns image id matching the image name."""
  data_mgr = ibmcloud_manager.ImageManager(GetGen(account))
  resp = data_mgr.List()['images']
  if resp is not None:
    for image in resp:
      if image['name'] == imgname:
        return image['id']
  return None


def GetImageIdInfo(account: Account, imageid: str):
  """Returns OS information matching the image id."""
  data_mgr = ibmcloud_manager.ImageManager(GetGen(account))
  return GetOsInfo(data_mgr.Show(imageid))


def GetOsInfo(image: Dict[Any, Any]) -> Dict[str, Any]:
  """Returns os information in json format.

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
    if 'custom' in image[
        'name']:  # this name is not the name in operating_system
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
