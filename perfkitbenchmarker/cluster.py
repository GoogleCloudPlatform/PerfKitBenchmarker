# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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

"""Class to represent a Cluster object."""

import os
import posixpath
import typing
from typing import Any, Callable, Iterator, List, Tuple
import uuid

from absl import flags
from absl import logging
from perfkitbenchmarker import data
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import resource
from perfkitbenchmarker import static_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import virtual_machine_spec
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.configs import vm_group_decoders
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands


FLAGS = flags.FLAGS
TEMPLATE_FILE = flags.DEFINE_string(
    'cluster_template_file',
    None,
    'The template file to be used to create the cluster. None by default, '
    'each provider has a default template file.',
)
UNMANAGED = flags.DEFINE_boolean(
    'cluster_unmanaged_provision',
    False,
    'Instead of creating with cluster toolset, relying on cloud provider CLI.'
    ' e.g. gcloud for gcp, awscli for aws.'
)
SCHEDULER = flags.DEFINE_enum(
    'cluster_scheduler',
    'slurm',
    ['slurm', 'k8s'],
    'Workload scheduler used by the cluster (slurm or k8s).',
)
TYPE = flags.DEFINE_string(
    'cluster_type',
    'default',
    'Type of cluster to use. Chances are clusters vary quite differently and '
    'may as well use its own template.'
)
_TRAINING_OPERATOR_MANIFEST_URL = (
    'github.com/kubeflow/training-operator/manifests/overlays/standalone'
)


class BaseClusterSpec(spec.BaseSpec):
  """Storing various data about HPC/ML cluster.

  Attributes:
    zone: The region / zone the in which to launch the cluster.
    machine_type: The provider-specific instance type (e.g. n1-standard-8).
    image: The disk image to boot from.
  """

  SPEC_TYPE = 'BaseClusterSpec'
  SPEC_ATTRS = ['CLOUD']
  CLOUD = None

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
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['cloud'].present:
      config_values['cloud'] = flag_values.cloud
    if flag_values['cluster_template_file'].present:
      config_values['template'] = flag_values.cluster_template_file
    if flag_values['cluster_unmanaged_provision'].present:
      config_values['unmanaged'] = flag_values.cluster_unmanaged_provision
    cloud = config_values.get('cloud')
    # only apply to workers
    if flag_values['num_vms'].present:
      config_values['workers']['vm_count'] = flag_values['num_vms'].value
    # flags should be applied to workers and headnode
    if flag_values['zone'].present:
      config_values['workers']['vm_spec'][cloud]['zone'] = flag_values[
          'zone'
      ].value[0]
      config_values['headnode']['vm_spec'][cloud]['zone'] = flag_values[
          'zone'
      ].value[0]
    for flag_name in ('os_type', 'cloud'):
      if flag_values[flag_name].present:
        config_values['workers'][flag_name] = flag_values[flag_name].value
        config_values['headnode'][flag_name] = flag_values[flag_name].value

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Can be overridden by derived classes to add options or impose additional
    requirements on existing options.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'workers': (vm_group_decoders.VmGroupSpecDecoder, {}),
        'headnode': (vm_group_decoders.VmGroupSpecDecoder, {}),
        'cloud': (option_decoders.StringDecoder, {'default': None}),
        'template': (option_decoders.StringDecoder, {'default': None}),
        'unmanaged': (option_decoders.BooleanDecoder, {'default': False}),
    })
    return result


def GetClusterSpecClass(cloud: str):
  """Returns the cluster spec class corresponding to the given service."""
  return spec.GetSpecClass(BaseClusterSpec, CLOUD=cloud)


def GetClusterClass(cloud: str):
  """Returns the cluster spec class corresponding to the given service."""
  if (
      UNMANAGED.value
      and TYPE.value == 'default'
      and SCHEDULER.value == 'slurm'
  ):
    return SlurmCluster
  return resource.GetResourceClass(
      BaseCluster,
      CLOUD=cloud,
      SCHEDULER=SCHEDULER.value,
      TYPE=TYPE.value,
  )


class BaseCluster(resource.BaseResource):
  """Base class for cluster resources.

  This class holds cluster-level methods and attributes.

  Attributes:
    machine_type: The provider-specific instance type for worker VMs.
    zone: The region / zone the VM was launched in.
  """

  RESOURCE_TYPE = 'BaseCluster'
  SCHEDULER = ''
  TYPE = 'default'
  REQUIRED_ATTRS = ['CLOUD', 'SCHEDULER', 'TYPE']
  DEFAULT_TEMPLATE = ''

  def __init__(self, cluster_spec: BaseClusterSpec):
    """Initialize BaseCluster class.

    Args:
      cluster_spec: cluster.BaseBaseClusterSpec object.
    """
    super().__init__()
    self.zone: str = cluster_spec.workers.vm_spec.zone
    self.unmanaged: bool = cluster_spec.unmanaged
    self.machine_type: str = cluster_spec.workers.vm_spec.machine_type
    self.template: str = cluster_spec.template or self.DEFAULT_TEMPLATE
    self.spec: BaseClusterSpec = cluster_spec
    self.worker_machine_type: str = self.machine_type
    self.headnode_machine_type: str = (
        cluster_spec.headnode.vm_spec.machine_type
    )
    self.headnode_spec: virtual_machine_spec.BaseVmSpec = (
        cluster_spec.headnode.vm_spec
    )
    self.workers_spec: virtual_machine_spec.BaseVmSpec = (
        cluster_spec.workers.vm_spec
    )
    self.num_workers: int = cluster_spec.workers.vm_count
    self.name: str = f'pkb{FLAGS.run_uri}'[:10]

  def DeployContainerImage(
      self,
      benchmark_name: str,
      dockerfile_relpath: str | None = None,
  ) -> str:
    """Prepares a workload container image (.sqsh on Slurm, OCI URI on K8s)."""
    raise NotImplementedError()

  def RunJob(
      self,
      benchmark_name: str,
      timeout: int = 7200,
      **template_vars: Any,
  ) -> Tuple[str, str]:
    """Renders and runs a benchmark job template, returning (stdout, stderr)."""
    raise NotImplementedError()

  def GetResourceMetadata(self) -> dict[str, Any]:
    return {
        'scheduler': self.SCHEDULER,
        'cluster_type': self.TYPE,
        'zone': self.zone,
        'machine_type': self.machine_type,
        'worker_machine_type': self.worker_machine_type,
        'headnode_machine_type': self.headnode_machine_type,
        'num_workers': self.num_workers,
        'template': self.template,
    }

  def __repr__(self):
    return f'<BaseCluster [name={self.name}]>'

  # TODO(yuyanting) Move common logic here after having concrete implementation.
  def _RenderClusterConfig(self):
    """Render the config file that will be used to create the cluster."""
    pass

  def _Create(self):
    pass

  def _Delete(self):
    pass


class SlurmCluster(BaseCluster):
  """Base class for VM/Slurm-based clusters."""

  SCHEDULER = 'slurm'

  def __init__(self, cluster_spec: BaseClusterSpec):
    """Initialize SlurmCluster class."""
    super().__init__(cluster_spec)
    self.image: str = cluster_spec.workers.vm_spec.image
    self.workers_static_disk_spec: disk.BaseDiskSpec = (
        cluster_spec.workers.disk_spec
    )
    self.workers_static_disk: static_virtual_machine.StaticDisk | None = (
        static_virtual_machine.StaticDisk(self.workers_static_disk_spec)
        if self.workers_static_disk_spec
        else None
    )
    self.os_type: str = cluster_spec.workers.os_type
    self.vms: List[linux_virtual_machine.BaseLinuxVirtualMachine] = []
    self.headnode_vm: linux_virtual_machine.BaseLinuxVirtualMachine | None = (
        None
    )
    self.worker_vms: List[linux_virtual_machine.BaseLinuxVirtualMachine] = []
    self.nfs_path: str = None

  def GetResourceMetadata(self) -> dict[str, Any]:
    metadata = super().GetResourceMetadata()
    metadata.update({
        'image': self.image,
        'os_type': self.os_type,
        'unmanaged': self.unmanaged,
    })
    return metadata

  def DeployContainerImage(
      self,
      benchmark_name: str,
      dockerfile_relpath: str | None = None,
  ) -> str:
    sqsh_name = f'{self.TYPE}.sqsh'
    dockerfile = dockerfile_relpath or posixpath.join(
        benchmark_name, f'{self.TYPE}.dockerfile'
    )
    self.InstallSquashImage(
        benchmark_name, sqsh_name, self.nfs_path, dockerfile
    )
    return posixpath.join(self.nfs_path, sqsh_name)

  def RunJob(
      self,
      benchmark_name: str,
      timeout: int = 7200,
      **template_vars: Any,
  ) -> Tuple[str, str]:
    template_path = posixpath.join(benchmark_name, 'slurm.sh.j2')
    try:
      data.ResourcePath(template_path)
    except data.ResourceNotFound as exc:
      raise errors.Benchmarks.UnsupportedConfigError(
          f'{benchmark_name} does not support Slurm template {template_path}'
      ) from exc
    srun_cmd = vm_util.ReadAndRenderJinja2Template(
        template_path, trim_spaces=True, **template_vars
    )
    return self.RemoteCommand(
        srun_cmd,
        login_shell=True,
        timeout=timeout,
        env=template_vars.get('env_export', ''),
    )

  def RemoteCommand(
      self,
      command: str,
      ignore_failure: bool = False,
      timeout: float | None = None,
      env: str = '',
      **kwargs,
  ) -> Tuple[str, str]:
    """Runs a command on the VM.

    Derived classes may add additional kwargs if necessary, but they should not
    be used outside of the class itself since they are non standard.

    Args:
      command: A valid bash command.
      ignore_failure: Ignore any failure if set to true.
      timeout: The time to wait in seconds for the command before exiting. None
        means no timeout.
      env: Environment variables to set before running the command.
      **kwargs: Additional command arguments.

    Returns:
      A tuple of stdout and stderr from running the command.

    Raises:
      RemoteCommandError: If there was a problem issuing the command.
    """
    return self.headnode_vm.RemoteCommand(
        f'{env} srun -N {self.num_workers} {command}',
        ignore_failure=ignore_failure,
        timeout=timeout,
        **kwargs,
    )

  def RobustRemoteCommand(
      self,
      command: str,
      timeout: float | None = None,
      ignore_failure: bool = False,
  ) -> Tuple[str, str]:
    """Runs a command on the VM in a more robust way than RemoteCommand.

    The default should be to call RemoteCommand and log that it is not yet
    implemented. This function should be overwritten it is decendents.

    Args:
      command: The command to run.
      timeout: The timeout for the command in seconds.
      ignore_failure: Ignore any failure if set to true.

    Returns:
      A tuple of stdout, stderr from running the command.

    Raises:
      RemoteCommandError: If there was a problem establishing the connection, or
          the command fails.
    """
    return self.headnode_vm.RobustRemoteCommand(
        command, ignore_failure=ignore_failure, timeout=timeout
    )

  def TryRemoteCommand(self, command: str, **kwargs):
    """Runs a remote command and returns True iff it succeeded."""
    try:
      self.RemoteCommand(command, **kwargs)
      return True
    except errors.VirtualMachine.RemoteCommandError:
      return False

  def InstantiateVm(self, vm_spec):
    """Creates VM object."""
    vm_class = virtual_machine.GetVmClass(vm_spec.CLOUD, self.os_type)
    return vm_class(vm_spec)

  def BackfillVm(
      self,
      vm_spec: virtual_machine_spec.BaseVmSpec,
      fn: Callable[[virtual_machine.BaseVirtualMachine], None],
  ):
    """Create and backfill a VM object created using cluster resource.

    Args:
      vm_spec: VM spec to be used to find corresponding VM class.
      fn: The function to be called on the newly created VM.

    Returns:
      The newly created VM object.
    """
    vm = self.InstantiateVm(vm_spec)
    fn(vm)
    vm.disks = []
    vm._PostCreate()  # pylint: disable=protected-access
    vm.created = True
    return vm

  def AuthenticateVM(self):
    """Authenticate a remote machine to access all vms."""
    for vm in self.vms:
      vm.AuthenticateVm()

  def ExportVmGroupsForUnmanagedProvision(self):
    """Export VmGroups for unmanaged provisioning.

    Returns:
      Dictionary of VmGroupSpec for provisioning in BenchmarkSpec object.
    """
    if not self.unmanaged:
      return {}
    logging.info('Provisioning cluster resources with unmanaged VM creation.')
    return {
        'headnode': self.spec.headnode,
        'workers': self.spec.workers,
    }

  def ImportVmGroups(self, headnode, workers):
    """Imports VMGroups from unmanaged provision.

    After VMs being created from unmanaged codepath. Add corresponding vm_groups
    back to cluster object. So the benchmark hopefully do not care about
    how underlying resources being created.

    Args:
      headnode: VirtualMachine object representing a headnode.
      workers: List of VirtualMachine objects representing workers.
    """
    self.headnode_vm = headnode
    self.worker_vms = workers
    self.vms = [self.headnode_vm] + self.worker_vms

  def _Create(self):
    pass

  def _Delete(self):
    pass

  def Create(self):
    if self.unmanaged:
      return
    super().Create()

  def Delete(self):
    if self.unmanaged:
      return
    super().Delete()

  @vm_util.Retry(
      fuzz=0,
      timeout=1800,
      max_retries=5,
      retryable_exceptions=(errors.Resource.RetryableCreationError,),
  )
  def _WaitForClusterReady(self):
    if self.unmanaged:
      return
    if not self.headnode_vm.TryRemoteCommand(
        f'srun -N {self.num_workers} hostname'
    ):
      raise errors.Resource.RetryableCreationError('Cluster not ready.')

  def InstallSquashImage(
      self,
      benchmark_name,
      image,
      install_path,
      dockerfile
  ):
    """Download squash image from preprovisoned bucket or built from scratch.

    Args:
      benchmark_name: The name of the benchmark defining the preprovisioned
        data. The benchmark's module must define the dict BENCHMARK_DATA mapping
        filenames to sha256sum hashes.
      image: String. Name of image stored in preprovisioned-bucket.
      install_path: The path to download the data file.
      dockerfile: String. Name of Dockerfile to built from scratch.
    """
    headnode = self.headnode_vm
    try:
      headnode.InstallPreprovisionedBenchmarkData(
          benchmark_name, [image], install_path
      )
    except errors.Setup.BadPreprovisionedDataError:
      logging.warning(
          'Cannot find preprovisioned squash image %s in preprovisioned bucket.'
          ' Attempting to build from dockerfile.',
          image,
      )
      headnode.PushFile(data.ResourcePath(dockerfile), 'Dockerfile')
      tmp_image = str(uuid.uuid4()).split('-')[0]
      image_path = os.path.join(install_path, image)
      headnode.RemoteCommand(
          f'docker build --network=host -t {tmp_image} .; '
          f'enroot import -o {image_path} dockerd://{tmp_image}; '
          f'docker rmi {tmp_image}'
      )

  def WaitForJobs(self) -> None:
    """Waits for cluster jobs to complete."""
    pass

  def GetInstallationNode(
      self,
  ) -> linux_virtual_machine.BaseLinuxVirtualMachine:
    """Returns the VM where software installation should happen."""
    return self.worker_vms[0]


class K8sCluster(BaseCluster):
  """Base class for Kubernetes-based clusters (GKE, EKS, CKS)."""

  SCHEDULER = 'k8s'

  def __init__(self, cluster_spec: BaseClusterSpec):
    """Initialize K8sCluster class."""
    super().__init__(cluster_spec)
    if not FLAGS.kubeconfig:
      FLAGS.kubeconfig = vm_util.PrependTempDir(f'kubeconfig{FLAGS.run_uri}')
    self.kubeconfig: str = FLAGS.kubeconfig
    self.k8s_version: str | None = None
    self.num_nodes: int = self.num_workers

  def GetResourceMetadata(self) -> dict[str, Any]:
    metadata = super().GetResourceMetadata()
    metadata.update({
        'num_nodes': self.num_nodes,
        'k8s_version': self.k8s_version,
    })
    return metadata

  def _PostCreate(self):
    super()._PostCreate()
    kubectl.RunKubectlCommand([
        'apply',
        '-k',
        _TRAINING_OPERATOR_MANIFEST_URL,
    ])

  def ApplyManifest(self, manifest_file: str, **kwargs: Any) -> Iterator[str]:
    """Applies a rendered Kubernetes manifest.

    Provided for backward compatibility with existing container_cluster
    benchmarks; ideally, benchmarks should only use RunJob.

    Args:
      manifest_file: The name of the YAML file or Jinja2 template.
      **kwargs: Arguments to pass to the Jinja2 template.

    Returns:
      An iterator of resource names (e.g. 'pod/my-pod') that were created.
    """
    return kubernetes_commands.ApplyManifest(manifest_file, **kwargs)

  def WaitForResource(
      self, resource_name: str, condition_name: str, **kwargs: Any
  ) -> Tuple[str, str, int]:
    """Waits for a condition on a Kubernetes resource.

    Provided for backward compatibility with existing container_cluster
    benchmarks; ideally, benchmarks should only use RunJob.

    Args:
      resource_name: The resource type/name (e.g. 'deploy/my-deploy').
      condition_name: The condition to wait for (e.g. 'Available').
      **kwargs: Additional arguments forwarded to kubernetes_commands.

    Returns:
      Tuple of (stdout, stderr, retcode).
    """
    return kubernetes_commands.WaitForResource(
        resource_name, condition_name, **kwargs
    )

  def WaitForSucceeded(
      self,
      resource_name: str,
      namespace: str | None = None,
      timeout: int = vm_util.DEFAULT_TIMEOUT,
      raise_on_failure: bool = True,
  ) -> Tuple[str, str, int]:
    """Waits for a Kubernetes resource to complete (jsonpath .status.succeeded=1).

    Provided for backward compatibility with existing container_cluster
    benchmarks; ideally, benchmarks should only use RunJob.

    Args:
      resource_name: The resource type/name to wait for.
      namespace: Optional Kubernetes namespace.
      timeout: Timeout in seconds.
      raise_on_failure: Whether to raise on timeout/failure.

    Returns:
      Tuple of (stdout, stderr, retcode).
    """
    return kubernetes_commands.WaitForSucceeded(
        resource_name,
        namespace=namespace,
        timeout=timeout,
        raise_on_failure=raise_on_failure,
    )


Cluster = typing.TypeVar('Cluster', bound=BaseCluster)
