# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Class to represent a Virtual Machine object.

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""

from typing import Any, Dict

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec

FLAGS = flags.FLAGS

GPU_K80 = 'k80'
GPU_P100 = 'p100'
GPU_V100 = 'v100'
GPU_A100 = 'a100'
GPU_H100 = 'h100'
GPU_B200 = 'b200'
GPU_P4 = 'p4'
GPU_P4_VWS = 'p4-vws'
GPU_T4 = 't4'
GPU_L4 = 'l4'
GPU_A10 = 'a10'
GPU_RTX_PRO_6000 = 'rtx-pro-6000'
TPU_V6E = 'v6e'
TESLA_GPU_TYPES = [
    GPU_K80,
    GPU_P100,
    GPU_V100,
    GPU_A100,
    GPU_P4,
    GPU_P4_VWS,
    GPU_T4,
    GPU_A10,
]
VALID_GPU_TYPES = TESLA_GPU_TYPES + [
    GPU_L4,
    GPU_H100,
    GPU_B200,
    GPU_RTX_PRO_6000,
    TPU_V6E,
]


def GetVmSpecClass(cloud: str, platform: str | None = None):
  """Returns the VmSpec class with corresponding attributes.

  Args:
    cloud: The cloud being used.
    platform: The vm platform being used (see enum above). If not provided,
      defaults to flag value.

  Returns:
    A child of BaseVmSpec with the corresponding attributes.
  """
  if platform is None:
    platform = provider_info.VM_PLATFORM.value
  return spec.GetSpecClass(BaseVmSpec, CLOUD=cloud, PLATFORM=platform)


class BaseVmSpec(spec.BaseSpec):
  """Storing various data about a single vm.

  Attributes:
    zone: The region / zone the in which to launch the VM.
    cidr: The CIDR subnet range in which to launch the VM.
    machine_type: The provider-specific instance type (e.g. n1-standard-8).
    gpu_count: None or int. Number of gpus to attach to the VM.
    gpu_type: None or string. Type of gpus to attach to the VM.
    image: The disk image to boot from.
    assign_external_ip: Bool.  If true, create an external (public) IP.
    install_packages: If false, no packages will be installed. This is useful if
      benchmark dependencies have already been installed.
    background_cpu_threads: The number of threads of background CPU usage while
      running the benchmark.
    background_network_mbits_per_sec: The number of megabits per second of
      background network traffic during the benchmark.
    background_network_ip_type: The IP address type (INTERNAL or EXTERNAL) to
      use for generating background network workload.
    disable_interrupt_moderation: If true, disables interrupt moderation.
    disable_rss: = If true, disables rss.
    boot_startup_script: Startup script to run during boot.
    vm_metadata: = Additional metadata for the VM.
  """

  SPEC_TYPE = 'BaseVmSpec'
  SPEC_ATTRS = ['CLOUD', 'PLATFORM']
  CLOUD = None
  PLATFORM = provider_info.DEFAULT_VM_PLATFORM

  def __init__(self, *args, **kwargs):
    self.boot_disk_size = None
    self.boot_disk_type: str | None = None  # Set by child classes.
    self.zone = None
    self.cidr = None
    self.machine_type: str
    self.gpu_count = None
    self.gpu_type = None
    self.image = None
    self.assign_external_ip = None
    self.install_packages = None
    self.background_cpu_threads = None
    self.background_network_mbits_per_sec = None
    self.background_network_ip_type = None
    self.disable_interrupt_moderation = None
    self.disable_rss = None
    self.vm_metadata: Dict[str, Any] = None
    self.boot_startup_script: str = None
    self.internal_ip: str
    super().__init__(*args, **kwargs)

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
    if flag_values['boot_disk_size'].present:
      config_values['boot_disk_size'] = flag_values.boot_disk_size
    if flag_values['image'].present:
      config_values['image'] = flag_values.image
    if flag_values['install_packages'].present:
      config_values['install_packages'] = flag_values.install_packages
    if flag_values['machine_type'].present:
      config_values['machine_type'] = flag_values.machine_type
    if flag_values['background_cpu_threads'].present:
      config_values['background_cpu_threads'] = (
          flag_values.background_cpu_threads
      )
    if flag_values['background_network_mbits_per_sec'].present:
      config_values['background_network_mbits_per_sec'] = (
          flag_values.background_network_mbits_per_sec
      )
    if flag_values['background_network_ip_type'].present:
      config_values['background_network_ip_type'] = (
          flag_values.background_network_ip_type
      )
    if flag_values['dedicated_hosts'].present:
      config_values['use_dedicated_host'] = flag_values.dedicated_hosts
    if flag_values['num_vms_per_host'].present:
      config_values['num_vms_per_host'] = flag_values.num_vms_per_host
    if flag_values['gpu_type'].present:
      config_values['gpu_type'] = flag_values.gpu_type
    if flag_values['gpu_count'].present:
      config_values['gpu_count'] = flag_values.gpu_count
    if flag_values['assign_external_ip'].present:
      config_values['assign_external_ip'] = flag_values.assign_external_ip
    if flag_values['disable_interrupt_moderation'].present:
      config_values['disable_interrupt_moderation'] = (
          flag_values.disable_interrupt_moderation
      )
    if flag_values['disable_rss'].present:
      config_values['disable_rss'] = flag_values.disable_rss
    if flag_values['vm_metadata'].present:
      config_values['vm_metadata'] = flag_values.vm_metadata
    if flag_values['boot_startup_script'].present:
      config_values['boot_startup_script'] = flag_values.boot_startup_script

    if 'gpu_count' in config_values and 'gpu_type' not in config_values:
      raise errors.Config.MissingOption(
          'gpu_type must be specified if gpu_count is set'
      )
    if 'gpu_type' in config_values and 'gpu_count' not in config_values:
      raise errors.Config.MissingOption(
          'gpu_count must be specified if gpu_type is set'
      )

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
        'boot_disk_size': (
            option_decoders.IntDecoder,
            {'none_ok': True, 'default': None},
        ),
        'disable_interrupt_moderation': (
            option_decoders.BooleanDecoder,
            {'default': False},
        ),
        'disable_rss': (option_decoders.BooleanDecoder, {'default': False}),
        'image': (
            option_decoders.StringDecoder,
            {'none_ok': True, 'default': None},
        ),
        'install_packages': (
            option_decoders.BooleanDecoder,
            {'default': True},
        ),
        'machine_type': (
            option_decoders.StringDecoder,
            {'none_ok': True, 'default': None},
        ),
        'assign_external_ip': (
            option_decoders.BooleanDecoder,
            {'default': True},
        ),
        'gpu_type': (
            option_decoders.EnumDecoder,
            {'valid_values': VALID_GPU_TYPES, 'default': None},
        ),
        'gpu_count': (
            option_decoders.IntDecoder,
            {'min': 1, 'default': None},
        ),
        'zone': (
            option_decoders.StringDecoder,
            {'none_ok': True, 'default': None},
        ),
        'cidr': (
            option_decoders.StringDecoder,
            {'none_ok': True, 'default': None},
        ),
        'use_dedicated_host': (
            option_decoders.BooleanDecoder,
            {'default': False},
        ),
        'num_vms_per_host': (option_decoders.IntDecoder, {'default': None}),
        'background_network_mbits_per_sec': (
            option_decoders.IntDecoder,
            {'none_ok': True, 'default': None},
        ),
        'background_network_ip_type': (
            option_decoders.EnumDecoder,
            {
                'default': vm_util.IpAddressSubset.EXTERNAL,
                'valid_values': [
                    vm_util.IpAddressSubset.EXTERNAL,
                    vm_util.IpAddressSubset.INTERNAL,
                ],
            },
        ),
        'background_cpu_threads': (
            option_decoders.IntDecoder,
            {'none_ok': True, 'default': None},
        ),
        'boot_startup_script': (
            option_decoders.StringDecoder,
            {'none_ok': True, 'default': None},
        ),
        'vm_metadata': (
            option_decoders.ListDecoder,
            {
                'item_decoder': option_decoders.StringDecoder(),
                'default': [],
            },
        ),
    })
    return result
