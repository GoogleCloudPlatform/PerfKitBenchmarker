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
"""Support for applying HPC optimized OS settings.

Applies HPC best practices for GCP as outlined in
https://cloud.google.com/solutions/best-practices-for-using-mpi-on-compute-engine
by running the script available here:
https://github.com/GoogleCloudPlatform/hpc-tools.git

HPC tools is only for RedHat (Centos) based OSes.  The PKB framework will call
YumInstall(vm) when vm.Install('gce_hpc_tools') is invoked.
"""

import logging
import posixpath
from absl import flags
from perfkitbenchmarker import vm_util

# Github URL for HPC tools used with flag --gce_hpc_tools
_HPC_URL = 'https://github.com/GoogleCloudPlatform/hpc-tools.git'
# Remote git checkout directory
_HPC_REMOTE_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'hpc-tools')
# HPC tools tuning script
_HPC_SCRIPT = 'mpi-tuning.sh'

flags.DEFINE_string('gce_hpc_tools_tag', None,
                    'Github tag of hpc-tools to use.  Default is latest.')
flags.DEFINE_list('gce_hpc_tools_tuning', [
    'hpcprofile', 'tcpmem', 'limits', 'nosmt', 'nofirewalld', 'noselinux',
    'nomitigation', 'reboot'
], 'List of HPC tunings.  `bash mpi-tuning.sh` for description.')

FLAGS = flags.FLAGS


def YumInstall(vm):
  """Applies the hpc-tools environment script.

  Optionally reboots.

  Args:
    vm: Virtual machine to apply HPC tools on.
  """
  tools_version = _CloneRepo(vm, FLAGS.gce_hpc_tools_tag)
  vm.metadata.update({
      'hpc_tools': True,
      'hpc_tools_tag': FLAGS.gce_hpc_tools_tag or 'head',
      'hpc_tools_version': tools_version,
      'hpc_tools_tuning': ','.join(sorted(FLAGS.gce_hpc_tools_tuning)),
  })
  logging.info('Applying hpc-tools to %s', vm)
  apply_command = f'cd {_HPC_REMOTE_DIR}; sudo bash {_HPC_SCRIPT}'
  for tuning in sorted(FLAGS.gce_hpc_tools_tuning):
    apply_command += f' --{tuning}'
  if 'reboot' in FLAGS.gce_hpc_tools_tuning:
    # Script will call reboot which makes a normal RemoteCommand fail.
    vm.RemoteCommand(apply_command, ignore_failure=True)
    vm.WaitForBootCompletion()
  else:
    vm.RemoteCommand(apply_command)


def _CloneRepo(vm, hpc_tools_tag):
  """Clones git repo, switches to tag, and returns current commit."""
  vm.InstallPackages('git')
  vm.RemoteCommand(f'rm -rf {_HPC_REMOTE_DIR}; '
                   f'git clone {_HPC_URL} {_HPC_REMOTE_DIR}')
  if hpc_tools_tag:
    vm.RemoteCommand(f'cd {_HPC_REMOTE_DIR}; git checkout {hpc_tools_tag}')
  stdout, _ = vm.RemoteCommand(
      f'cd {_HPC_REMOTE_DIR}; git log --pretty=format:"%h" -n 1')
  return stdout.splitlines()[-1]
