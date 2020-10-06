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
"""Class to represent a IBM Cloud Virtual Machine. """

import base64
import json
import logging
import threading
import os
import time
import yaml
import linecache

from subprocess import Popen, PIPE

from perfkitbenchmarker import context
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.ibmcloud import ibmcloud as ibm
from perfkitbenchmarker.providers.ibmcloud import ibmcloud_disk
from perfkitbenchmarker.providers.ibmcloud import utils

WINDOWS = 'windows'  # all windows
UBUNTU = 'ubuntu'  # all ubuntu
REDHAT = 'redhat'  # all redhat
DEBIAN = 'debian'  # all debian
CENTOS = 'centos'  # all centos
UNKNOWN = 'unknown'

FLAGS = flags.FLAGS

# these are used to create extra subnets for multi vnic support in perfkit,
# not used for vm provisioning, up to 4 extra totaling 5 subnets
SUBNET_SUFFIX = 's'
SUBNET_SUFFIX_EXTRA = 'sxs'
DELIMITER = 'z-z'

# each prefix range of /18 can only house 4 subnets with /20, use /16, 0: zone1, 1: zone2, etc
VPC_NAME = 'vpc'
VPC_PREFIX_RANGES = ['10.101.0.0/16', '10.102.0.0/16', '10.103.0.0/16', '10.104.0.0/16', '10.105.0.0/16']

VPC_SUBNETS = ['10.101.0.0/20', '10.102.0.0/20', '10.103.0.0/20', '10.104.0.0/20', '10.105.0.0/20']

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

USER_DATA = "Content-Type: multipart/mixed; boundary=MIMEBOUNDARY\n\
MIME-Version: 1.0\n\
--MIMEBOUNDARY\n\
Content-Type: text/cloud-config; charset=\"us-ascii\"\n\
MIME-Version: 1.0\n\
Content-Transfer-Encoding: 7bit\n\
Content-Disposition: attachment; filename=\"cloud-config\"\n\
#cloud-config\n\
set_timezone: America/Chicago\n\
--MIMEBOUNDARY\n\
Content-Type: text/x-shellscript; charset=\"us-ascii\"\n\
MIME-Version: 1.0\n\
Content-Transfer-Encoding: 7bit\n\
Content-Disposition: attachment; filename=\"set-content.ps1\"\n\
#ps1_sysnative\n\
Set-Content -Path \"C:\\helloWorld.txt\" -Value \"Hello, World!\"\n\
--MIMEBOUNDARY\n\
Content-Type: text/x-shellscript; charset=\"us-ascii\"\n\
MIME-Version: 1.0\n\
Content-Transfer-Encoding: 7bit\n\
Content-Disposition: attachment; filename=\"set-content.ps1\"\n\
#ps1_sysnative\n\
function Setup-Remote-Desktop () {\n\
Set-ItemProperty \"HKLM:\\SYSTEM\\CurrentControlSet\\Control\\Terminal Server\" -Name fDenyTSConnections -Value 0\n\
Set-ItemProperty \"HKLM:\\SYSTEM\\CurrentControlSet\\Control\\Terminal Server\\WinStations\\RDP-Tcp\" -Name \"UserAuthentication\" -Value 1\n\
Enable-NetFireWallRule -DisplayGroup \"Remote Desktop\"\n}\n\
function Setup-Ping () {\n\
Set-NetFirewallRule -DisplayName \"File and Printer Sharing (Echo Request - ICMPv4-In)\" -enabled True\n\
Set-NetFirewallRule -DisplayName \"File and Printer Sharing (Echo Request - ICMPv6-In)\" -enabled True\n}\n\
Setup-Remote-Desktop\n\
Setup-Ping\n\
New-NetFirewallRule -DisplayName \"Allow iperf 5201\" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5201\n\
New-NetFirewallRule -DisplayName \"Allow iperf 5202\" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5202\n\
New-NetFirewallRule -DisplayName \"Allow iperf 5203\" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5203\n\
New-NetFirewallRule -DisplayName \"Allow iperf 5204\" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5204\n\
New-NetFirewallRule -DisplayName \"Allow iperf 5205\" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5205\n\
New-NetFirewallRule -DisplayName \"Allow netperf 20000\" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 20000\n\
New-NetFirewallRule -DisplayName \"Allow netperf 20001\" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 20001\n\
New-NetFirewallRule -DisplayName \"Allow netperf 20002\" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 20002\n\
New-NetFirewallRule -DisplayName \"Allow netperf 20003\" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 20003\n\
New-NetFirewallRule -DisplayName \"Allow netperf 20010\" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 20010\n\
New-NetFirewallRule -DisplayName \"Allow netperf 20011\" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 20011\n\
New-NetFirewallRule -DisplayName \"Allow netperf 20012\" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 20012\n\
New-NetFirewallRule -DisplayName \"Allow netperf 20013\" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 20013\n\
New-NetFirewallRule -DisplayName \"Allow winrm https 5986\" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5986\n\
winrm set winrm/config/service/auth '@{Basic=\"true\";Certificate=\"true\"}'\n\
$cert=New-SelfSignedCertificate -certstorelocation cert:\localmachine\my -dnsname *\n\
$thumb=($cert).Thumbprint\n\
New-WSManInstance -ResourceURI winrm/config/Listener -SelectorSet @{Address=\"*\";Transport=\"HTTPS\"} -ValueSet @{CertificateThumbprint=\"$thumb\"}\n\
powercfg /SetActive (powercfg /List | %{if ($_.Contains(\"High performance\")){$_.Split()[3]}})\n\
Set-NetAdapterAdvancedProperty -Name Ethernet -RegistryKeyword MTU -RegistryValue 9000\n\
--MIMEBOUNDARY--"


def ReadConfig(config):
  """Reads in config yml """
  try:
    with open(config, 'r') as stream:
      data = json.dumps(yaml.load(stream), sort_keys=True)
      return json.loads(data)
  except Exception as ex:
    logging.error('Failed to load configuration file %s, %s', config, ex)
    os._exit(1)  # just exit here, no need to continue


def GetSubnetIndex(ipv4_cidr_block):
  """Finds the index for the cidr """
  for name in SUBNETXS:
    ip_list = SUBNETS_EXTRA[name]
    for i in range(len(ip_list)):
      if ip_list[i] == ipv4_cidr_block:
        return i
  return -1


def GetRouteCommands(data, index, target_index):
  """Creates a list of ip route commands to run """
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
  """Find min/max """
  if not data:
    return 0.0, 0.0
  min = float(data[0])
  max = float(data[0])
  for item in data:
    value = float(item)
    if value > max:
      max = value
    if value < min:
      min = value
  return min, max


def GetBaseOs(os):
  """Finds the base os name to use """
  if 'name' in os and os['name'] is not None:
    osname = os['name'].lower()
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
  gen = ibm.IbmCloud(account=account[0], apikey=account[1], verbose=False, \
                     version='v1', silent=True, force=True)
  if not gen.token():
    gen.set_token()  # one more try
  return gen


def GetImageId(account, imgname):
  """Finds image id based on image name """
  data_mgr = ibm.ImageManager(GetGen(account))
  resp = data_mgr.List()['images']
  if resp is not None:
    for image in resp:
      if image['name'] == imgname:
        return image['id']
  return None


def GetImageIdInfo(account, imageid):
  """Finds basic image info """
  data_mgr = ibm.ImageManager(GetGen(account))
  return GetOsInfo(data_mgr.Show(imageid))


def GetOsInfo(image):
  """Finds OS info """
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


class IbmCloudVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a IBM Cloud Virtual Machine"""

  CLOUD = providers.IBMCLOUD
  IMAGE_NAME_PREFIX = None

  _lock = threading.Lock()
  _lock_vpc = threading.Lock()
  command_works = False
  ibmcloud_apikey = None
  ibmcloud_account_id = None
  validated_resources_set = set()
  validated_subnets = 0  # indicator for number of subnets created

  def __init__(self, vm_spec):
    """Initialize a IBM Cloud virtual machine.
    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(IbmCloudVirtualMachine, self).__init__(vm_spec)
    self.user_name = FLAGS.ibmcloud_image_username
    self.boot_volume_size = FLAGS.ibmcloud_boot_volume_size
    self.boot_volume_iops = FLAGS.ibmcloud_boot_volume_iops
    self.volume_iops = 3000
    if FLAGS.ibmcloud_volume_iops:
      self.volume_iops = FLAGS.ibmcloud_volume_iops
    self.volume_profile = FLAGS.ibmcloud_volume_profile
    self.imageid = FLAGS.ibmcloud_image_id
    self.os_data = None
    self.user_data = None
    self.vmid = None
    self.vm_deleted = False
    self.vm_started = False
    self.instance_start_failed = True
    self.instance_stop_failed = False
    self.profile = FLAGS.ibmcloud_profile
    self.prefix = FLAGS.ibmcloud_prefix
    self.zone = None
    self.fip_address = None
    self.fip_id = None
    self.ssh_pub_keyfile = None
    self.subnet = FLAGS.ibmcloud_subnet
    self.subnets = {}
    self.vpcid = FLAGS.ibmcloud_vpcid
    self.key = FLAGS.ibmcloud_pub_keyid
    self.sgid = None
    self.encryption_key = FLAGS.ibmcloud_bootvol_encryption_key if \
    FLAGS.ibmcloud_bootvol_encryption_key else None
    self.extra_vdisks_created = False
    self.device_paths_detected = set()

  def _CreateRiasKey(self):
    """creates a ibmcloud key from the generated ssh key """
    logging.info('Creating rias key')
    with open(vm_util.GetPublicKeyPath(), 'r') as keyfile:
      pubkey = keyfile.read()
    logging.info('ssh private key file: %s, public key file: %s', \
                 vm_util.GetPrivateKeyPath(), vm_util.GetPublicKeyPath())
    cmd = utils.IbmAPICommand(self)
    cmd.flags['name'] = self.prefix + str(flags.FLAGS.run_uri) + 'key'
    cmd.flags['pubkey'] = pubkey
    return cmd.CreateKey()

  def _CheckImage(self):
    """Verifies we have an imageid to use """
    if FLAGS.ibmcloud_image_id:
      logging.info('Image id to use: %s', FLAGS.ibmcloud_image_id)
    else:
      cmd = utils.IbmAPICommand(self)
      cmd.flags['image_name'] = FLAGS.ibmcloud_image_name if \
      FLAGS.ibmcloud_image_name else self._GetDefaultImageName()
      logging.info('Looking up image: %s', cmd.flags['image_name'])
      self.imageid = cmd.GetImageId()
      if self.imageid is None:
        logging.info('Failed to find valid image id')
        os._exit(1)
      else:
        logging.info('Image id found: %s', self.imageid)

  @classmethod
  def _GetDefaultImageName(cls):
    """Returns the default image name prefx """
    return cls.IMAGE_NAME_PREFIX

  def _SetupMzone(self):
    """Looks up the resources needed, if not found, creates new """
    logging.info('Checking mzone setup')
    cmd = utils.IbmAPICommand(self)
    cmd.flags['prefix'] = self.prefix
    cmd.flags['zone'] = self.zone
    cmd.flags['items'] = 'vpcs'
    self.vpcid = cmd.ListResources()
    logging.info('Vpc found: %s', self.vpcid)

    cmd.flags['items'] = 'subnets'
    self.subnet = cmd.ListResources()
    logging.info('Subnet found: %s', self.subnet)
    subnet_index = int(self.zone[len(self.zone) - 1])  # get the ending -1
    if not self.vpcid:
      logging.info('Creating a vpc')
      cmd.flags['name'] = self.prefix + FLAGS.run_uri + 'vpc'
      self.vpcid = cmd.CreateVpc()
      if self.vpcid:
        # let first thread create address prefix for all zones
        zone_list = [str(item) for item in FLAGS.ibmcloud_zones.split(',')]
        for zone in zone_list:
          cmd.flags['vpcid'] = self.vpcid
          cmd.flags['zone'] = zone
          index = int(zone[len(zone) - 1])  # get the ending -1
          cmd.flags['cidr'] = VPC_PREFIX_RANGES[index - 1]
          cmd.flags['name'] = self.prefix + VPC_NAME + DELIMITER + zone
          resp = cmd.CreatePrefix()
          if resp:
            logging.info('Created vpc prefix range: %s', resp.get('id'))
          else:
            logging.info('Failed to create vpc prefix range, exiting')
            os._exit(1)
      else:
        logging.error('Failed to create vpc, exiting')
        os._exit(1)

    if not self.subnet:
      cmd.flags['vpcid'] = self.vpcid
      cmd.flags['zone'] = self.zone
      cmd.flags['name'] = self.prefix + VPC_NAME + SUBNET_SUFFIX + \
      str(subnet_index) + DELIMITER + self.zone
      cmd.flags['cidr'] = VPC_SUBNETS[subnet_index - 1]
      logging.info('Creating subnet: %s', cmd.flags)
      resp = cmd.CreateSubnet()
      if resp:
        self.subnet = resp.get('id')
        IbmCloudVirtualMachine.validated_subnets += 1
        logging.info('Created subnet: %s, zone %s', self.subnet, self.zone)
      else:
        logging.error('Failed to create subnet, exiting')
        os._exit(1)

    if FLAGS.ibmcloud_subnets_extra > 0:
      cmd.flags['prefix'] = SUBNET_SUFFIX_EXTRA
      self.subnets = cmd.ListSubnetsExtra()
      logging.info('Extra subnets found: %s', self.subnets)

    # look up for exsting key that matches this run uri
    cmd.flags['items'] = 'keys'
    cmd.flags['prefix'] = self.prefix + FLAGS.run_uri
    self.key = cmd.ListResources()
    logging.info('Key found: %s', self.key)
    if self.key is None:
      cmd.flags['items'] = 'keys'
      cmd.flags['prefix'] = self.prefix + FLAGS.run_uri
      self.key = self._CreateRiasKey()
      if self.key is None:
        logging.info('Failed to create a new key')
        os._exit(1)
      logging.info('Created a new key: %s', self.key)

    if not self.vpcid or not self.subnet or not self.key:
      raise errors.Error('IBM Cloud ERROR: Failed to lookup resources')

    logging.info('Looking up the image: %s', self.imageid)
    cmd.flags['imageid'] = self.imageid
    self.os_data = GetOsInfo(cmd.ImageShow())
    logging.info('Image os: %s', self.os_data)
    logging.info('Checking mzone setup finished')

  def _CleanUp(self):
    """
    deletes the used resources, subnet, vpc, rias key
    """
    with self._lock:
      if IbmCloudVirtualMachine.validated_subnets > 0:
        cmd = utils.IbmAPICommand(self)
        cmd.flags['items'] = 'subnets'
        cmd.flags['id'] = self.subnet
        logging.info('Waiting to delete subnet')
        time.sleep(30)
        cmd.DeleteResource()
        IbmCloudVirtualMachine.validated_subnets -= 1

    # different lock so all threads get a chance to delete its subnet if different
    with self._lock_vpc:
      if self.vpcid not in IbmCloudVirtualMachine.validated_resources_set:
        cmd = utils.IbmAPICommand(self)
        # check till all subnets are gone
        time_to_end = time.time() + 300
        while IbmCloudVirtualMachine.validated_subnets > 0:
          logging.info('Subnets not empty yet')
          if time_to_end < time.time():
            break
          time.sleep(10)
        cmd.flags['items'] = 'vpcs'
        cmd.flags['id'] = self.vpcid
        logging.info('Pausing before deleting vpc')
        time.sleep(10)
        cmd.DeleteResource()
        IbmCloudVirtualMachine.validated_resources_set.add(self.vpcid)
      # key is not dependent on vpc, one key is used
      if self.key not in IbmCloudVirtualMachine.validated_resources_set:
        cmd = utils.IbmAPICommand(self)
        cmd.flags['items'] = 'keys'
        cmd.flags['id'] = self.key
        cmd.DeleteResource()
        IbmCloudVirtualMachine.validated_resources_set.add(self.key)

  def _Create(self):
    """Creates and starts a IBM Cloud VM instance."""
    self._CreateInstance()
    if self.subnet:  # this is for the primary vnic and fip
      self.fip_address, self.fip_id = self._CreateFip(self.name + 'fip')
      self.ip_address = self.fip_address
      self.internal_ip = self._WaitForIPAssignment(self.subnet)
      logging.info('Fip: %s, ip: %s', self.ip_address, self.internal_ip)

    if self.subnets:
      # create the extra vnics
      cmd = utils.IbmAPICommand(self)
      cmd.flags['instanceid'] = self.vmid
      for subnet_name in self.subnets.keys():
        cmd.flags['name'] = subnet_name
        cmd.flags['subnet'] = self.subnets[subnet_name]['id']  # subnet id
        logging.info('Creating extra vnic for vmid: %s, subnet: %s', \
                     self.vmid, cmd.flags['subnet'])
        vnicid, ip_addr = cmd.InstanceVnicCreate()
        logging.info('Extra vnic created for vmid: %s, vnicid: %s, ip_addr: %s',
                     self.vmid, vnicid, ip_addr)
        self.subnets[subnet_name]['vnicid'] = vnicid
        self.subnets[subnet_name]['ip_addr'] = ip_addr
      logging.info('Extra vnics created for vmid: %s, subnets: %s',
                   self.vmid, self.subnets)

  def _Delete(self):
    """Delete all the resources that were created """
    if self.vm_deleted:
      return
    if not utils.IbmAPICommand.ibmcloud_auth_token or \
    time.time() - utils.IbmAPICommand.ibmcloud_auth_token_time > FLAGS.ibmcloud_timeout:
      self._CheckLogin()
    if self.fip_address:
      self._DeleteFip()
    self._StopInstance()
    self._DeleteInstance()
    if not FLAGS.ibmcloud_resources_keep:
      self._CleanUp()

  def _DeleteDependencies(self):
    """Delete dependencies that were needed for the VM after the VM has been
    deleted."""
    pass

  def _Exists(self):
    if self.vm_deleted:
      return False
    if self.instance_stop_failed:
      return False
    return True

  def _CreateDependencies(self):
    """Validate and Create dependencies prior creating the VM."""
    self._CheckPrerequisites()

  def _CheckPrerequisites(self):
    """Checks prerequisites are met otherwise aborts execution."""
    self._CheckCanaryCommand()
    with self._lock:
      logging.info('')
      logging.info('Validating prerequisites.')
      logging.info('zones: %s', FLAGS.ibmcloud_zones)
      if ',' in FLAGS.ibmcloud_zones:
        zone_list = [str(item) for item in FLAGS.ibmcloud_zones.split(',')]
        for zone in zone_list:
          if zone not in IbmCloudVirtualMachine.validated_resources_set:
            self.zone = zone
            break
      else:
        self.zone = FLAGS.ibmcloud_zones
      logging.info('zone to use %s', self.zone)
      self._CheckImage()
      self._SetupMzone()
      IbmCloudVirtualMachine.validated_resources_set.add(self.zone)
      logging.info('Prerequisites validated.')

  def _CheckCanaryCommand(self):
    """Checks that the IBM Cloud CLI is working, and as a side effect gets
    an auth-token for use on later CLI commands."""
    if IbmCloudVirtualMachine.command_works:  # fast path
      return
    with self._lock:
      if IbmCloudVirtualMachine.command_works:
        return
      self._CheckLogin()

  def _CheckLogin(self):
    if not os.environ.get('IBMCLOUD_ENDPOINT'):
      raise errors.Config.InvalidValue(
        'PerfKit Benchmarker on IBM Cloud requires that the '
        'environment variable IBMCLOUD_ENDPOINT is set.')
    self._GetAuthenticationInformation()
    try:
      cmd = utils.IbmAPICommand(self, 'login')
      cmd.flags['apikey'] = IbmCloudVirtualMachine.ibmcloud_apikey
      if FLAGS.ibmcloud_login_validfrom:
        cmd.flags['validfrom'] = FLAGS.ibmcloud_login_validfrom
      cmd.flags['leaseduration'] = '24h'
    except Exception as exc:
      raise errors.Config.InvalidValue(
        'IBM Cloud API test command failed. Please make sure the IBM Cloud '
        'environment variable IBMCLOUD_ENDPOINT and IBMCLOUD_AUTH_ENDPOINT '
        'are correctly set and that the correct values for authentication '
        'were supplied. IBM Cloud ERROR: {0}'.format(exc))
    utils.IbmAPICommand.ibmcloud_auth_token = cmd.GetToken()
    utils.IbmAPICommand.ibmcloud_auth_token_time = time.time()
    IbmCloudVirtualMachine.command_works = True

  def _GetAuthenticationInformation(self):
    """Get the information needed to authenticate on IBM Cloud."""
    account_id = os.environ.get('IBMCLOUD_ACCOUNT_ID')
    apikey = os.environ.get('IBMCLOUD_APIKEY')
    if not account_id or not apikey:
      accounts = os.environ.get('IBMCLOUD_ACCOUNTS')  # read from config file
      if accounts is not None and accounts != '':
        config = self._ReadConfig(accounts)
        account_id = config[FLAGS.benchmarks[0]][0][0]  # first account
        apikey = config[FLAGS.benchmarks[0]][0][1]
        if len(config[FLAGS.benchmarks[0]][0]) > 2:  # override the rgid if set
          FLAGS.ibmcloud_rgid = config[FLAGS.benchmarks[0]][0][2]
    if not account_id or not apikey:
      raise errors.Config.InvalidValue(
          'PerfKit Benchmarker on IBM Cloud requires that the '
          'environment variables IBMCLOUD_ACCOUNT_ID and IBMCLOUD_APIKEY '
          'are correctly set.')
    utils.IbmAPICommand.ibmcloud_account_id = account_id
    utils.IbmAPICommand.ibmcloud_apikey = apikey
    IbmCloudVirtualMachine.ibmcloud_account_id = account_id
    IbmCloudVirtualMachine.ibmcloud_apikey = apikey

  def _ReadConfig(self, config):
    try:
      with open(config, 'r') as stream:
        data = json.dumps(yaml.load(stream), sort_keys=True)
        return json.loads(data)
    except Exception as ex:
      logging.info('Failed to load accounts file %s, %s', config, ex)
      os._exit(1)

  def _CreateInstance(self):
    """Execute command for creating a IBM Cloud VM instance."""
    cmd = utils.IbmAPICommand(self)
    cmd.flags['name'] = self.name
    cmd.flags['imageid'] = self.imageid
    cmd.flags['profile'] = self.profile
    cmd.flags['vpcid'] = self.vpcid
    cmd.flags['subnet'] = self.subnet
    cmd.flags['key'] = self.key
    cmd.flags['zone'] = self.zone
    cmd.user_data = self.user_data
    if self.boot_volume_size > 0:
      cmd.flags['capacity'] = self.boot_volume_size
    cmd.flags['iops'] = self.boot_volume_iops
    if self.encryption_key:
      cmd.flags['encryption_key'] = self.encryption_key
    logging.info('Creating instance, flags: %s', cmd.flags)
    resp = json.loads(cmd.CreateInstance())
    if 'id' not in resp:
      raise errors.Error('IBM Cloud ERROR: Failed to create instance: %s', resp)
    self.vmid = resp['id']
    logging.info('Instance created, id: %s', self.vmid)
    logging.info('Waiting for instance to start, id: %s', self.vmid)
    cmd.flags['instanceid'] = self.vmid
    status = cmd.InstanceStatus()
    self.instance_start_failed = False
    assert status == utils.RUNNING
    if status != utils.RUNNING:
      logging.error("Instance start failed, status %s" % status)
      self.instance_start_failed = True
    self.vm_started = not self.instance_start_failed
    logging.info('Instance %s status %s', self.vmid, status)

  def _StartInstance(self):
    """Execute command for starting a IBM Cloud VM instance."""
    cmd = utils.IbmAPICommand(self)
    cmd.flags['instanceid'] = self.vmid
    status = cmd.InstanceStart()
    logging.info('start_instance_poll: last status is %s', status)
    self.instance_start_failed = False
    assert status == utils.RUNNING
    if status != utils.RUNNING:
      logging.error("Instance start failed, status %s" % status)
      self.instance_start_failed = True
    self.vm_started = not self.instance_start_failed

  def _CreateFip(self, name):
    """Execute command for creating a VNIC in a IBM Cloud VM instance."""
    # instance should be running when we get here
    cmd = utils.IbmAPICommand(self)
    cmd.flags['instanceid'] = self.vmid
    vnicid = cmd.InstanceGetPrimaryVnic()
    cmd.flags['name'] = name
    cmd.flags['target'] = vnicid
    logging.info('Creating FIP for instanceid: %s', self.vmid)
    resp = json.loads(cmd.InstanceFipCreate())
    if resp:
      logging.info('FIP create resp: %s', resp)
      assert resp['address'] != None
      return resp['address'], resp['id']
    logging.error('FAILED to create FIP for instance %s', self.vmid)
    return None, None

  def _DeleteFip(self):
    logging.info('Deleting FIP, instanceid: %s, fip address: %s, fip id: %s',
                 self.vmid, self.fip_address, self.fip_id)
    cmd = utils.IbmAPICommand(self)
    cmd.flags['fip_id'] = self.fip_id
    cmd.InstanceFipDelete()

  def _FindVnicIdByName(self, vnics, name):
    for vnic in vnics:
      if vnic['name'] == name:
        return vnic['uid']
    return None

  def _FindVnicIdBySubnet(self, vnics, subnet):
    for vnic in vnics:
      if vnic['subnet']['id'] == subnet:
        return vnic['id']
    return None

  def _WaitForIPAssignment(self, networkid):
    """Poll to get the IP address assigned by DHCP."""
    IPv4Address = '0.0.0.0'
    count = 0
    while IPv4Address == '0.0.0.0' and \
    count * FLAGS.ibmcloud_polling_delay < 240:
      time.sleep(FLAGS.ibmcloud_polling_delay)
      count += 1
      cmd = utils.IbmAPICommand(self)
      cmd.flags['instanceid'] = self.vmid
      logging.info('Looking for IP for instance %s, networkid: %s',
                   self.vmid, networkid)

      resp = cmd.InstanceShow()
      for network in resp['network_interfaces']:
        if network['subnet']['id'] == networkid:
          IPv4Address = network['primary_ipv4_address']
          break
      logging.info('Waiting on ip assignment: %s', IPv4Address)
    assert IPv4Address != '0.0.0.0'
    return IPv4Address

  def _StopInstance(self):
    """Execute command for stopping a IBM Cloud VM instance."""
    cmd = utils.IbmAPICommand(self)
    cmd.flags['instanceid'] = self.vmid
    status = cmd.InstanceStop()
    logging.info('stop_instance_poll: last status is %s', status)
    self.instance_stop_failed = False
    if status != utils.STOPPED:
      logging.error("Instance stop failed: status %s" % status)
    self.vm_started = False

  def _DeleteInstance(self):
    cmd = utils.IbmAPICommand(self)
    cmd.flags['instanceid'] = self.vmid
    cmd.InstanceDelete()
    self.vm_deleted = True
    logging.info('Instance deleted: %s', cmd.flags['instanceid'])

  def CreateScratchDisk(self, disk_spec):
    disks_names = ('%s-data-%d-%d'
                   % (self.name, len(self.scratch_disks), i)
                   for i in range(disk_spec.num_striped_disks))
    disks = [ibmcloud_disk.IbmCloudDisk(disk_spec, name, self.zone)
             for name in disks_names]

    self._CreateScratchDiskFromDisks(disk_spec, disks)

  def DownloadPreprovisionedData(self, install_path, module_name, filename):
    """Creats a temp file, no download """
    self.RemoteCommand('echo "1234567890" > ' + \
                       os.path.join(install_path, filename))

  def ShouldDownloadPreprovisionedData(self, module_name, filename):
    """Returns whether or not preprovisioned data is available."""
    return True


class Debian9BasedIbmCloudVirtualMachine(IbmCloudVirtualMachine,
                                         linux_virtual_machine.Debian9Mixin):
  IMAGE_NAME_PREFIX = 'ibm-debian-9-'

  def PrepareVMEnvironment(self):
    logging.info('Pausing for 2 min before update and installs')
    time.sleep(120)
    self.RemoteCommand('DEBIAN_FRONTEND=noninteractive apt-get -y update')
    self.RemoteCommand('DEBIAN_FRONTEND=noninteractive apt-get -y install sudo')
    super(Debian9BasedIbmCloudVirtualMachine, self).PrepareVMEnvironment()


class Ubuntu1604BasedIbmCloudVirtualMachine(IbmCloudVirtualMachine,
                                            linux_virtual_machine.Ubuntu1604Mixin):
  IMAGE_NAME_PREFIX = 'ibm-ubuntu-16-04-'

  def PrepareVMEnvironment(self):
    self.RemoteCommand('DEBIAN_FRONTEND=noninteractive apt-get -y update')
    super(Ubuntu1604BasedIbmCloudVirtualMachine, self).PrepareVMEnvironment()


class Ubuntu1804BasedIbmCloudVirtualMachine(IbmCloudVirtualMachine,
                                            linux_virtual_machine.Ubuntu1804Mixin):
  IMAGE_NAME_PREFIX = 'ibm-ubuntu-18-04-'

  def PrepareVMEnvironment(self):
    logging.info('Pausing for 10 min before update and installs')
    time.sleep(600)
    self.RemoteCommand('DEBIAN_FRONTEND=noninteractive apt-get -y update')
    super(Ubuntu1804BasedIbmCloudVirtualMachine, self).PrepareVMEnvironment()


class Rhel7BasedIbmCloudVirtualMachine(IbmCloudVirtualMachine,
                                       linux_virtual_machine.Rhel7Mixin):
  IMAGE_NAME_PREFIX = 'ibm-redhat-7-6-minimal-amd64'

  def PrepareVMEnvironment(self):
    logging.info('Pausing for 5 min before update and installs')
    time.sleep(300)
    super(Rhel7BasedIbmCloudVirtualMachine, self).PrepareVMEnvironment()


class WindowsIbmCloudVirtualMachine(IbmCloudVirtualMachine,
                                    windows_virtual_machine.BaseWindowsMixin):
  """Support for Windows machines on IBMCloud"""

  def __init__(self, vm_spec):
    super(WindowsIbmCloudVirtualMachine, self).__init__(vm_spec)
    self.user_name = 'Administrator'
    self.user_data = USER_DATA

  @vm_util.Retry()
  def _GetDecodedPasswordData(self):
    # Retrieve a base64 encoded, encrypted password for the VM.
    cmd = utils.IbmAPICommand(self)
    cmd.flags['instanceid'] = self.vmid
    resp = cmd.InstanceInitializationShow()
    logging.info('Instance %s, resp %s', self.vmid, resp)
    encrypted = None
    if resp and 'password' in resp and 'encrypted_password' in resp['password']:
      encrypted = resp['password']['encrypted_password']
    if encrypted is None:
      raise ValueError('Failed to retrieve encrypted password')
    return base64.b64decode(encrypted)

  def _PostCreate(self):
    """Retrieve generic VM info and then retrieve the VM's password."""
    super(WindowsIbmCloudVirtualMachine, self)._PostCreate()

    # Get the decoded password data.
    decoded_password_data = self._GetDecodedPasswordData()

    # Write the encrypted data to a file, and use openssl to
    # decrypt the password.
    with vm_util.NamedTemporaryFile() as tf:
      tf.write(decoded_password_data)
      tf.close()
      decrypt_cmd = ['openssl',
                     'rsautl',
                     '-decrypt',
                     '-in',
                     tf.name,
                     '-inkey',
                    vm_util.GetPrivateKeyPath()]
      password, _ = vm_util.IssueRetryableCommand(decrypt_cmd)
      self.password = password
    logging.info('Password decrypted for %s, %s', self.fip_address, self.vmid)


class Windows2012CoreIbmCloudVirtualMachine(WindowsIbmCloudVirtualMachine,
                                            windows_virtual_machine.Windows2012CoreMixin):
  IMAGE_NAME_PREFIX = 'ibm-windows-server-2012-full'


class Windows2016CoreIbmCloudVirtualMachine(WindowsIbmCloudVirtualMachine,
                                            windows_virtual_machine.Windows2016CoreMixin):
  IMAGE_NAME_PREFIX = 'ibm-windows-server-2016-full'


class Windows2019CoreIbmCloudVirtualMachine(WindowsIbmCloudVirtualMachine,
                                            windows_virtual_machine.Windows2019CoreMixin):
  IMAGE_NAME_PREFIX = 'ibm-windows-server-2019-full'
