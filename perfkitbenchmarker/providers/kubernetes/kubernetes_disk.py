# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

import json
import logging
import re

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import errors
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker.vm_util import OUTPUT_STDOUT as STDOUT,\
    OUTPUT_STDERR as STDERR, OUTPUT_EXIT_CODE as EXIT_CODE
from perfkitbenchmarker.configs import option_decoders

FLAGS = flags.FLAGS


def CreateDisks(disk_specs, vm_name):
  """
  Creates instances of KubernetesDisk child classes depending on
  scratch disk type.
  """
  scratch_disks = []
  for disk_num, disk_spec in enumerate(disk_specs):
    disk_class = GetKubernetesDiskClass(disk_spec.disk_type)
    scratch_disk = disk_class(disk_num, disk_spec, vm_name)
    scratch_disk.Create()
    scratch_disks.append(scratch_disk)
  return scratch_disks


class KubernetesDiskSpec(disk.BaseDiskSpec):

  CLOUD = providers.KUBERNETES

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    result = super(KubernetesDiskSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'provisioner': (option_decoders.StringDecoder,
                        {'default': None, 'none_ok': True}),
        'parameters': (option_decoders.TypeVerifier,
                       {'default': {}, 'valid_types': (dict,)})
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Overrides config values with flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. Is
          modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
          provided config values.

    Returns:
      dict mapping config option names to values derived from the config
      values or flag values.
    """
    super(KubernetesDiskSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['k8s_volume_provisioner'].present:
      config_values['provisioner'] = flag_values.k8s_volume_provisioner
    if flag_values['k8s_volume_parameters'].present:
      config_values['parameters'] = config_values.get('parameters', {})
      config_values['parameters'].update(
          flag_util.ParseKeyValuePairs(flag_values.k8s_volume_parameters))


def GetKubernetesDiskClass(volume_type):
  return resource.GetResourceClass(KubernetesDisk, K8S_VOLUME_TYPE=volume_type)


class KubernetesDisk(disk.BaseDisk):
  """
  Base class for Kubernetes Disks.
  """

  RESOURCE_TYPE = 'KubernetesDisk'
  REQUIRED_ATTRS = ['K8S_VOLUME_TYPE']

  def __init__(self, disk_num, disk_spec, name):
    super(KubernetesDisk, self).__init__(disk_spec)
    self.name = '%s-%s' % (name, disk_num)

  def _Create(self):
    return

  def _Delete(self):
    return

  def Attach(self, vm):
    return

  def Detach(self):
    return

  def SetDevicePath(self, vm):
    return

  def AttachVolumeMountInfo(self, volume_mounts):
    volume_mount = {
        "mountPath": self.mount_point,
        "name": self.name
    }
    volume_mounts.append(volume_mount)


class EmptyDirDisk(KubernetesDisk):
  """
  Implementation of Kubernetes 'emptyDir' type of volume.
  """

  K8S_VOLUME_TYPE = 'emptyDir'

  def GetDevicePath(self):
    """
    In case of LocalDisk, host's disk is mounted (empty directory from the
    host is mounted to the docker instance) and we intentionally
    prevent from formatting the device.
    """
    raise errors.Error('GetDevicePath not supported for Kubernetes local disk')

  def AttachVolumeInfo(self, volumes):
    local_volume = {
        "name": self.name,
        "emptyDir": {}
    }
    volumes.append(local_volume)


class CephDisk(KubernetesDisk):
  """
  Implementation of Kubernetes 'rbd' type of volume.
  """

  K8S_VOLUME_TYPE = 'rbd'

  def __init__(self, disk_num, disk_spec, name):
    super(CephDisk, self).__init__(disk_num, disk_spec, name)
    self.ceph_secret = FLAGS.ceph_secret

  def _Create(self):
    """
    Creates Rados Block Device volumes and installs filesystem on them.
    """
    cmd = ['rbd', '-p', FLAGS.rbd_pool, 'create', self.name, '--size',
           str(1024 * self.disk_size)]
    output = vm_util.IssueCommand(cmd)
    if output[EXIT_CODE] != 0:
      raise Exception("Creating RBD image failed: %s" % output[STDERR])

    cmd = ['rbd', 'map', FLAGS.rbd_pool + '/' + self.name]
    output = vm_util.IssueCommand(cmd)
    if output[EXIT_CODE] != 0:
      raise Exception("Mapping RBD image failed: %s" % output[STDERR])
    rbd_device = output[STDOUT].rstrip()
    if '/dev/rbd' not in rbd_device:
      # Sometimes 'rbd map' command doesn't return any output.
      # Trying to find device location another way.
      cmd = ['rbd', 'showmapped']
      output = vm_util.IssueCommand(cmd)
      for image_device in output[STDOUT].split('\n'):
        if self.name in image_device:
          pattern = re.compile("/dev/rbd.*")
          output = pattern.findall(image_device)
          rbd_device = output[STDOUT].rstrip()
          break

    cmd = ['/sbin/mkfs.ext4', rbd_device]
    output = vm_util.IssueCommand(cmd)
    if output[EXIT_CODE] != 0:
      raise Exception("Formatting partition failed: %s" % output[STDERR])

    cmd = ['rbd', 'unmap', rbd_device]
    output = vm_util.IssueCommand(cmd)
    if output[EXIT_CODE] != 0:
      raise Exception("Unmapping block device failed: %s" % output[STDERR])

  def _Delete(self):
    """
    Deletes RBD image.
    """
    cmd = ['rbd', 'rm', FLAGS.rbd_pool + '/' + self.name]
    output = vm_util.IssueCommand(cmd)
    if output[EXIT_CODE] != 0:
      msg = "Removing RBD image failed. Reattempting."
      logging.warning(msg)
      raise Exception(msg)

  def AttachVolumeInfo(self, volumes):
    ceph_volume = {
        "name": self.name,
        "rbd": {
            "monitors": FLAGS.ceph_monitors,
            "pool": FLAGS.rbd_pool,
            "image": self.name,
            "keyring": FLAGS.ceph_keyring,
            "user": FLAGS.rbd_user,
            "fsType": "ext4",
            "readOnly": False
        }
    }
    if FLAGS.ceph_secret:
      ceph_volume["rbd"]["secretRef"] = {"name": FLAGS.ceph_secret}
    volumes.append(ceph_volume)

  def SetDevicePath(self, vm):
    """
    Retrieves the path to scratch disk device.
    """
    cmd = "mount | grep %s | tr -s ' ' | cut -f 1 -d ' '" % self.mount_point
    device, _ = vm.RemoteCommand(cmd)
    self.device_path = device.rstrip()

  def GetDevicePath(self):
    return self.device_path


class PersistentVolumeClaim(resource.BaseResource):
  """Object representing a K8s PVC."""

  @vm_util.Retry(poll_interval=10, max_retries=100, log_errors=False)
  def _WaitForPVCBoundCompletion(self):
    """
    Need to wait for the PVC to get up  - PVC may take some time to be ready(Bound).
    """
    exists_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig, 'get',
                  'pvc', '-o=json', self.name]
    logging.info("Waiting for PVC %s" % self.name)
    pvc_info, _, _ = vm_util.IssueCommand(exists_cmd, suppress_warning=True)
    if pvc_info:
      pvc_info = json.loads(pvc_info)
      pvc = pvc_info['status']['phase']
      if pvc == "Bound":
        logging.info("PVC is ready.")
        return
    raise Exception("PVC %s is not ready. Retrying to check status." %
                    self.name)

  def __init__(self, name, storage_class, size):
    super(PersistentVolumeClaim, self).__init__()
    self.name = name
    self.storage_class = storage_class
    self.size = size

  def _Create(self):
    """Creates the PVC."""
    body = self._BuildBody()
    kubernetes_helper.CreateResource(body)
    self._WaitForPVCBoundCompletion()

  def _Delete(self):
    """Deletes the PVC."""
    body = self._BuildBody()
    kubernetes_helper.DeleteResource(body)

  def _BuildBody(self):
    """Builds JSON representing the PVC."""
    body = {
        'kind': 'PersistentVolumeClaim',
        'apiVersion': 'v1',
        'metadata': {
            'name': self.name
        },
        'spec': {
            'accessModes': ['ReadWriteOnce'],
            'resources': {
                'requests': {
                    'storage': '%sGi' % self.size
                }
            },
            'storageClassName': self.storage_class,
        }
    }
    return json.dumps(body)


class StorageClass(resource.BaseResource):
  """Object representing a K8s StorageClass (with dynamic provisioning)."""

  def __init__(self, name, provisioner, parameters):
    super(StorageClass, self).__init__()
    self.name = name
    self.provisioner = provisioner
    self.parameters = parameters

  def _CheckStorageClassExists(self):
    """
    Prevent duplicated StorageClass creation If the StorageClass with the same name and parameters exists
    :return: True or False
    """

    body = self._BuildBody()
    exists_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig, 'get',
                  'sc', '-o=json', self.name]

    sc_info, _, _ = vm_util.IssueCommand(exists_cmd, suppress_warning=True)
    if sc_info:
      sc_info = json.loads(sc_info)
      sc_name = sc_info['metadata']['name']
      if sc_name == self.name:
        logging.info("StorageClass already exists.")
        return True
      else:
        logging.info("About to create new StorageClass: {0}".format(self.name))
        return False

  def _Create(self):
    """Creates the PVC."""
    body = self._BuildBody()
    if not self._CheckStorageClassExists():
      kubernetes_helper.CreateResource(body)

  def _Delete(self):
    """Deletes the PVC."""
    body = self._BuildBody()
    kubernetes_helper.DeleteResource(body)

  def _BuildBody(self):
    """Builds JSOM representing the StorageClass."""
    body = {
        'kind': 'StorageClass',
        'apiVersion': 'storage.k8s.io/v1',
        'metadata': {
            'name': self.name
        },
        'provisioner': self.provisioner,
        'parameters': self.parameters
    }
    return json.dumps(body)


class PvcVolume(KubernetesDisk):
  """Volume representing a persistent volume claim."""

  K8S_VOLUME_TYPE = 'persistentVolumeClaim'
  PROVISIONER = None

  def __init__(self, disk_num, spec, name):
    super(PvcVolume, self).__init__(disk_num, spec, name)
    self.storage_class = StorageClass(
        name, self.PROVISIONER or spec.provisioner, spec.parameters)
    self.pvc = PersistentVolumeClaim(
        self.name, self.storage_class.name, spec.disk_size)

  def _Create(self):
    self.storage_class.Create()
    self.pvc.Create()

  def _Delete(self):
    self.pvc.Delete()
    self.storage_class.Delete()

  def AttachVolumeInfo(self, volumes):
    pvc_volume = {
        'name': self.name,
        'persistentVolumeClaim': {
            'claimName': self.name
        }
    }
    volumes.append(pvc_volume)
