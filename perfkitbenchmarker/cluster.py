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

from collections.abc import Mapping
import os
import posixpath
import re
import shutil
import typing
from typing import Any, Callable, List, Tuple
import uuid

from absl import flags
from absl import logging
import jinja2
from perfkitbenchmarker import data
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import resource
from perfkitbenchmarker import static_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import virtual_machine_spec
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec as container_spec_lib
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.configs import vm_group_decoders
from perfkitbenchmarker.resources.container_service import container_registry
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands


FLAGS = flags.FLAGS
_PREPROVISIONED_CONTAINER_REGISTRY = flags.DEFINE_string(
    'preprovisioned_container_registry',
    None,
    'Optional OCI container registry repository prefix or full image URI '
    'for K8sCluster workloads. Why: avoids building heavy GPU/HPC Dockerfiles '
    'on every run and enables fast pulls (e.g. GKE Image Streaming from '
    'Artifact Registry). Behavior: (1) If set to a repository prefix without '
    'a tag (e.g. "us-central1-docker.pkg.dev/my-project/my-repo"), '
    'K8sCluster.DeployContainerImage resolves the image to '
    '"<prefix>/<benchmark_name>_<cluster_type>:latest" (e.g. '
    '".../nccl_cluster_a4:latest"). (2) If set to a full image URI with an '
    'explicit ":<tag>" in the final path component (e.g. '
    '"us-central1-docker.pkg.dev/my-project/my-repo/custom_nccl:v1"), that '
    'exact URI is used verbatim. (3) If unset (None), K8sCluster creates an '
    'ephemeral cloud container registry, builds and pushes '
    'data/<benchmark_name>/<cluster_type>.dockerfile at runtime, and deletes '
    'the registry during teardown.',
)
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
    ' e.g. gcloud for gcp, awscli for aws.',
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
    'may as well use its own template.',
)


class BaseClusterSpec(spec.BaseSpec):
  """Storing various data about HPC/ML cluster.

  Note that `headnode` and `default_pool` are scheduler-specific optional
  sections that may coexist in a benchmark config:
  - `SlurmCluster` reads `headnode` and ignores `default_pool`.
  - `K8sCluster` reads `default_pool` and ignores `headnode`.

  Attributes:
    zone: The region / zone the in which to launch the cluster.
    machine_type: The provider-specific instance type (e.g. n1-standard-8).
    image: The disk image to boot from.
  """

  SPEC_TYPE = 'BaseClusterSpec'
  SPEC_ATTRS = ['CLOUD']
  CLOUD = None
  workers: vm_group_decoders.VmGroupSpec
  headnode: vm_group_decoders.VmGroupSpec | None
  default_pool: vm_group_decoders.VmGroupSpec | None
  cloud: str | None
  template: str | None
  unmanaged: bool

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
    # flags should be applied to workers and headnode/default_pool (if present)
    if flag_values['zone'].present:
      config_values['workers']['vm_spec'][cloud]['zone'] = flag_values[
          'zone'
      ].value[0]
      for group_key in ('headnode', 'default_pool'):
        if config_values.get(group_key):
          config_values[group_key]['vm_spec'][cloud]['zone'] = flag_values[
              'zone'
          ].value[0]
    for flag_name in ('os_type', 'cloud'):
      if flag_values[flag_name].present:
        config_values['workers'][flag_name] = flag_values[flag_name].value
        for group_key in ('headnode', 'default_pool'):
          if config_values.get(group_key):
            config_values[group_key][flag_name] = flag_values[flag_name].value

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
        'headnode': (
            vm_group_decoders.VmGroupSpecDecoder,
            {'default': None, 'none_ok': True},
        ),
        'default_pool': (
            vm_group_decoders.VmGroupSpecDecoder,
            {'default': None, 'none_ok': True},
        ),
        'cloud': (option_decoders.StringDecoder, {'default': None}),
        'template': (option_decoders.StringDecoder, {'default': None}),
        'unmanaged': (option_decoders.BooleanDecoder, {'default': False}),
    })
    return result


def GetClusterSpecClass(cloud: str):
  """Returns the cluster spec class corresponding to the given service."""
  return spec.GetSpecClass(BaseClusterSpec, CLOUD=cloud)


def GetClusterClass(cloud: str):
  """Returns the cluster class corresponding to the given cloud."""
  if UNMANAGED.value and SCHEDULER.value == 'k8s':
    raise errors.Config.InvalidValue(
        '--cluster_unmanaged_provision is not supported with '
        '--cluster_scheduler=k8s.'
    )
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
  DEFAULT_JOB_TEMPLATE = ''
  JOB_TEMPLATE_EXT = ''
  DEFAULT_JOB_ENV: Mapping[str, str] = {}

  def __init__(self, cluster_spec: BaseClusterSpec):
    """Initialize BaseCluster class.

    Args:
      cluster_spec: cluster.BaseClusterSpec object.
    """
    super().__init__()
    self.zone: str = cluster_spec.workers.vm_spec.zone
    self.unmanaged: bool = cluster_spec.unmanaged
    self.machine_type: str = cluster_spec.workers.vm_spec.machine_type
    self.template: str = cluster_spec.template or self.DEFAULT_TEMPLATE
    self.spec: BaseClusterSpec = cluster_spec
    self.worker_machine_type: str = self.machine_type
    self.workers_spec: virtual_machine_spec.BaseVmSpec = (
        cluster_spec.workers.vm_spec
    )
    self.num_workers: int = cluster_spec.workers.vm_count
    self.name: str = f'pkb{FLAGS.run_uri}'[:10]
    self._config: str = ''
    self._container_images: dict[str, str] = {}

  def DeployContainerImage(
      self,
      benchmark_name: str,
      dockerfile_relpath: str | None = None,
  ) -> str:
    """Prepares a workload container image (.sqsh on Slurm, OCI URI on K8s)."""
    raise NotImplementedError()

  def _PrepareJobTemplateAndContext(
      self,
      benchmark_name: str | None,
      job_name: str,
      command: str | None,
      env: Mapping[str, str] | None,
      **template_vars: Any,
  ) -> Tuple[str, dict[str, Any]]:
    """Resolves the job template path and builds the Jinja2 rendering context."""
    template_name = f'{job_name}{self.JOB_TEMPLATE_EXT}'
    if benchmark_name and data.ResourceExists(
        posixpath.join(benchmark_name, template_name)
    ):
      template_path = posixpath.join(benchmark_name, template_name)
    else:
      # Generic templates are stored in data/cluster/.
      template_path = posixpath.join('cluster', template_name)
      if template_path == self.DEFAULT_JOB_TEMPLATE and not command:
        raise ValueError(
            'command is required when using default template '
            f'{self.DEFAULT_JOB_TEMPLATE}.'
        )

    merged_env = {**self.DEFAULT_JOB_ENV, **(env or {})}
    context = {
        'job_name': job_name,
        'num_workers': self.num_workers,
        'env': merged_env,
        'command': command,
        **template_vars,
    }
    if image := template_vars.get('image') or (
        self._container_images.get(benchmark_name) if benchmark_name else None
    ):
      context['image'] = image
    return template_path, context

  def RunJob(
      self,
      benchmark_name: str | None = None,
      job_name: str = '',
      command: str | None = None,
      env: Mapping[str, str] | None = None,
      timeout: int = 7200,
      **template_vars: Any,
  ) -> Tuple[str, str]:
    """Renders and runs a benchmark job template, returning (stdout, stderr)."""
    raise NotImplementedError()

  def GetResourceMetadata(self) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        'scheduler': self.SCHEDULER,
        'cluster_type': self.TYPE,
        'zone': self.zone,
        'machine_type': self.machine_type,
        'worker_machine_type': self.worker_machine_type,
        'num_workers': self.num_workers,
        'template': self.template,
    }
    if self._container_images:
      metadata['container_images'] = self._container_images
    return metadata

  def __repr__(self):
    return f'<BaseCluster [name={self.name}]>'

  def _GetTemplateParameters(self) -> dict[str, Any]:
    """Returns parameters that will be used to render the cluster config."""
    raise NotImplementedError()

  def _CreateDependencies(self):
    """Method that will be called once before _CreateResource() is called.

    Supplying this method is optional. It is intended to allow additional
    flexibility in creating resource dependencies separately from _Create().
    """
    self._RenderClusterConfig()

  def _RenderClusterConfig(self):
    """Render the config file that will be used to create the cluster."""
    with open(data.ResourcePath(self.template)) as content:
      template = jinja2.Template(
          content.read(), undefined=jinja2.StrictUndefined
      )
      self._config = template.render(self._GetTemplateParameters())

  def _Create(self):
    pass

  def _Delete(self):
    pass


class SlurmCluster(BaseCluster):
  """Base class for VM/Slurm-based clusters.

  Reads `headnode` (`headnode_spec`, `headnode_machine_type`) from the cluster
  spec and ignores `default_pool`.
  """

  SCHEDULER = 'slurm'
  JOB_TEMPLATE_EXT = '.sbatch.j2'
  DEFAULT_JOB_TEMPLATE = f'cluster/slurm_job{JOB_TEMPLATE_EXT}'
  DEFAULT_JOB_ENV: Mapping[str, str] = {}

  def __init__(self, cluster_spec: BaseClusterSpec):
    """Initialize SlurmCluster class."""
    super().__init__(cluster_spec)
    if cluster_spec.headnode is None:
      raise errors.Config.InvalidValue(
          'SlurmCluster requires a headnode configuration.'
      )
    self.headnode_spec: virtual_machine_spec.BaseVmSpec = (
        cluster_spec.headnode.vm_spec
    )
    self.headnode_machine_type: str = (
        self.headnode_spec.machine_type
    )
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
        'headnode_machine_type': self.headnode_machine_type,
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
    image_path = posixpath.join(self.nfs_path, sqsh_name)
    self._container_images[benchmark_name] = image_path
    return image_path

  def RunJob(
      self,
      benchmark_name: str | None = None,
      job_name: str = 'slurm_job',
      command: str | None = None,
      env: Mapping[str, str] | None = None,
      timeout: int = 7200,
      **template_vars: Any,
  ) -> Tuple[str, str]:
    """Renders an sbatch script to the headnode and runs it synchronously."""
    assert self.headnode_vm is not None
    job_dir = self.nfs_path or '.'
    remote_script_path = posixpath.join(job_dir, f'{job_name}.sbatch')
    output_pattern = posixpath.join(job_dir, 'slurm-%j.out')
    template_vars.setdefault('output_path', output_pattern)
    template_path, context = self._PrepareJobTemplateAndContext(
        benchmark_name, job_name, command, env, **template_vars
    )
    self.headnode_vm.RenderTemplate(
        data.ResourcePath(template_path),
        remote_script_path,
        context,
        should_log_file=True,
    )
    stdout, _ = self.headnode_vm.RemoteCommand(
        f'sbatch --wait {remote_script_path}',
        login_shell=True,
        timeout=timeout,
    )
    match = re.search(r'Submitted batch job (\d+)', stdout)
    if not match:
      raise errors.VirtualMachine.RemoteCommandError(
          f'Failed to parse Slurm batch job ID from sbatch output: {stdout}'
      )
    output_path = posixpath.join(job_dir, f'slurm-{match.group(1)}.out')
    self.headnode_vm.PullFile(vm_util.GetTempDir(), output_path)
    return self.headnode_vm.RemoteCommand(f'cat {output_path}')

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

  def _PostCreate(self):
    super()._PostCreate()
    self.AuthenticateVM()

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
      dockerfile,
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
  """Base class for Kubernetes-based clusters (GKE, EKS, CKS).

  Reads `default_pool` (`default_pool_spec`, `default_pool_machine_type`) from
  the cluster spec and ignores `headnode`.
  """

  SCHEDULER = 'k8s'
  JOB_TEMPLATE_EXT = '.yaml.j2'
  DEFAULT_JOB_TEMPLATE = f'cluster/k8s_job{JOB_TEMPLATE_EXT}'
  DEFAULT_JOB_ENV: Mapping[str, str] = {}

  def __init__(self, cluster_spec: BaseClusterSpec):
    """Initialize K8sCluster class."""
    super().__init__(cluster_spec)
    if cluster_spec.default_pool is None:
      raise errors.Config.InvalidValue(
          'K8sCluster requires a default_pool configuration.'
      )
    self.default_pool_spec: virtual_machine_spec.BaseVmSpec = (
        cluster_spec.default_pool.vm_spec
    )
    self.default_pool_machine_type: str = (
        self.default_pool_spec.machine_type
    )
    if not FLAGS.kubeconfig:
      FLAGS.kubeconfig = vm_util.PrependTempDir(f'kubeconfig{FLAGS.run_uri}')
    self.kubeconfig: str = FLAGS.kubeconfig
    self.k8s_version: str | None = None
    self.num_nodes: int = self.num_workers
    self._container_registry: (
        container_registry.BaseContainerRegistry | None
    ) = None

  def _DeleteDependencies(self):
    super()._DeleteDependencies()
    if self._container_registry is not None:
      self._container_registry.Delete()

  def GetResourceMetadata(self) -> dict[str, Any]:
    metadata = super().GetResourceMetadata()
    metadata.update({
        'default_pool_machine_type': self.default_pool_machine_type,
        'num_nodes': self.num_nodes,
        'k8s_version': self.k8s_version,
    })
    return metadata

  def DeployContainerImage(
      self,
      benchmark_name: str,
      dockerfile_relpath: str | None = None,
  ) -> str:
    """Resolves a preprovisioned OCI image or builds and pushes the Dockerfile.

    Resolution follows two paths based on `--preprovisioned_container_registry`:
    1. Preprovisioned fast path (`--preprovisioned_container_registry` is set):
       - If the flag value's last path segment includes an explicit `:<tag>`
         (e.g. `us-central1-docker.pkg.dev/my-proj/my-repo/nccl_a4:v1`), that
         URI is used verbatim.
       - Otherwise, the flag value is treated as a registry/repository prefix
         and resolved to `<prefix>/<benchmark_name>_<self.TYPE>:latest` (e.g.
         `us-central1-docker.pkg.dev/my-proj/my-repo/nccl_cluster_a4:latest`).
       - No Dockerfile build or ephemeral registry provisioning takes place.
    2. Ephemeral build fallback (`--preprovisioned_container_registry` is None):
       - Stages `dockerfile_relpath` (defaults to
         `<benchmark_name>/<self.TYPE>.dockerfile` in PKB `data/`) into a
         temporary build directory.
       - Lazily creates an ephemeral cloud `BaseContainerRegistry`
         (e.g. `GoogleArtifactRegistry` on GCP or `ElasticContainerRegistry`
         on AWS) that is automatically deleted in `_DeleteDependencies()`.
       - Builds and pushes `<benchmark_name>_<self.TYPE>` via
         `self._container_registry.GetOrBuild(...)`.

    The resolved OCI image URI is cached in
    `self._container_images[benchmark_name]` so subsequent
    `RunJob(benchmark_name, ...)` calls automatically inject `image` into the
    Kubernetes manifest template context.

    Args:
      benchmark_name: Name of the benchmark requesting the image (e.g.
        'nccl_cluster').
      dockerfile_relpath: Optional relative path under PKB `data/` to the
        Dockerfile. Defaults to `<benchmark_name>/<self.TYPE>.dockerfile`.

    Returns:
      The resolved OCI image URI string for use in Kubernetes pod specs.
    """
    image_name = f'{benchmark_name}_{self.TYPE}'
    if _PREPROVISIONED_CONTAINER_REGISTRY.value:
      registry_val = _PREPROVISIONED_CONTAINER_REGISTRY.value.rstrip('/')
      if ':' in registry_val.split('/')[-1]:
        image_uri = registry_val
      else:
        image_uri = f'{registry_val}/{image_name}:latest'
      self._container_images[benchmark_name] = image_uri
      return image_uri

    dockerfile = dockerfile_relpath or posixpath.join(
        benchmark_name, f'{self.TYPE}.dockerfile'
    )
    build_dir = os.path.join(vm_util.GetTempDir(), 'docker', image_name)
    os.makedirs(build_dir, exist_ok=True)
    shutil.copyfile(
        data.ResourcePath(dockerfile), os.path.join(build_dir, 'Dockerfile')
    )
    if self._container_registry is None:
      registry_spec = container_spec_lib.ContainerRegistrySpec(
          'container_registry', flag_values=FLAGS, cloud=self.CLOUD
      )
      registry_spec.name = f'pkb{FLAGS.run_uri}'
      registry_spec.zone = self.zone
      registry_cls = container_registry.GetContainerRegistryClass(self.CLOUD)
      self._container_registry = registry_cls(registry_spec)
      self._container_registry.Create()

    image_uri = self._container_registry.GetOrBuild(image_name, build_dir)
    self._container_images[benchmark_name] = image_uri
    return image_uri

  def RunJob(
      self,
      benchmark_name: str | None = None,
      job_name: str = 'k8s_job',
      command: str | None = None,
      env: Mapping[str, str] | None = None,
      timeout: int = 7200,
      **template_vars: Any,
  ) -> Tuple[str, str]:
    """Renders a Kubernetes job manifest and runs it synchronously."""
    template_path, context = self._PrepareJobTemplateAndContext(
        benchmark_name, job_name, command, env, **template_vars
    )
    if template_path == self.DEFAULT_JOB_TEMPLATE and 'image' not in context:
      raise ValueError(
          'image is required when using default template '
          f'{self.DEFAULT_JOB_TEMPLATE}.'
      )
    k8s_job_name = job_name.replace('_', '-')
    context['job_name'] = k8s_job_name
    context.setdefault('worker_machine_type', self.worker_machine_type)

    resources: list[str] = []
    try:
      resources = list(
          kubernetes_commands.ApplyManifest(template_path, **context)
      )
      condition = kubernetes_commands.WaitForResourceForMultiConditions(
          f'job/{k8s_job_name}',
          ['condition=complete', 'condition=failed'],
          timeout=timeout,
      )
      stdout, stderr, _ = kubectl.RunKubectlCommand([
          'logs',
          '-l',
          f'job-name={k8s_job_name},batch.kubernetes.io/job-completion-index=0',
          '--tail=-1',
      ])
      if condition == 'condition=failed':
        raise errors.VirtualMachine.RemoteCommandError(
            f'Kubernetes job {k8s_job_name} failed.\n'
            f'STDOUT: {stdout}\nSTDERR: {stderr}'
        )
      return stdout, stderr
    finally:
      if resources:
        kubectl.RunKubectlCommand(
            ['delete', *resources, '--ignore-not-found=true', '--wait=true'],
            raise_on_failure=False,
        )


Cluster = typing.TypeVar('Cluster', bound=BaseCluster)
