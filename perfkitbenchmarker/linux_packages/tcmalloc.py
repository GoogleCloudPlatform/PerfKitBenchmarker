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
"""Module containing tcmalloc shared library installation function.

The tcmalloc library will be installed by setting LD_PRELOAD to preload
libtcmalloc.so.  To enable tcmalloc, please add `vm.Install('tcmalloc')` to your
benchmark script and pass `--tcmalloc_version` at runtime. Other flags may also
be needed. Refer to the flags below for details.

Note: This package is only available for Debian VMs on GCP now. Modification is
needed for other platforms.
"""

from absl import flags
from perfkitbenchmarker import errors

flags.DEFINE_enum('tcmalloc_version', 'off',
                  ['off', 'gperftools', 'experimental'],
                  'the tcmalloc version to be preloaded')
flags.DEFINE_string(
    'tcmalloc_experimental_url', '',
    'the GCS URL for downloading the tcmalloc experimental lib')
flags.DEFINE_string(
    'tcmalloc_settings',
    '',
    'tcmalloc settings modifying runtime behavior as environment variables '
    'such as "ARG1=foo,ARG2=bar", see more: '
    'https://gperftools.github.io/gperftools/tcmalloc.html',
)

FLAGS = flags.FLAGS

TEMP_BASHRC = '/tmp/bash.bashrc'
BASHRC = '/etc/bash.bashrc'


def AptInstall(vm):
  """Installs the tcmalloc shared library on a Debian VM."""
  if FLAGS.tcmalloc_version == 'off':
    return

  # Write tcmalloc settings as environment variables
  settings = FLAGS.tcmalloc_settings.split(',')
  for setting in settings:
    if setting:
      vm.RemoteCommand('echo "export {setting}" | sudo tee -a {tmp}'.format(
          setting=setting,  # e.g. 'TCMALLOC_RELEASE_RATE=0.5'
          tmp=TEMP_BASHRC,
      ))

  if FLAGS.tcmalloc_version == 'gperftools':
    vm.InstallPackages('libgoogle-perftools-dev')
    libtcmalloc_paths = [
        '/usr/lib/libtcmalloc.so.4',  # before v2.7
        '/usr/lib/x86_64-linux-gnu/libtcmalloc.so.4',  # since v2.7
    ]
    vm.RemoteCommand(
        'test -f {path1} '
        '&& echo "export LD_PRELOAD={path1}" | sudo tee -a {tmp} '
        '|| echo "export LD_PRELOAD={path2}" | sudo tee -a {tmp} '.format(
            path1=libtcmalloc_paths[0],
            path2=libtcmalloc_paths[1],
            tmp=TEMP_BASHRC,
        ))

  if FLAGS.tcmalloc_version == 'experimental':
    vm.Install('google_cloud_sdk')
    local_path = '/tmp/libtcmalloc.so'
    vm.RemoteCommand(
        'gsutil cp {url} {path} '
        '&& echo "export LD_PRELOAD={path}" | sudo tee -a {tmp}'.format(
            url=FLAGS.tcmalloc_experimental_url,
            path=local_path,
            tmp=TEMP_BASHRC))

  # The environment variables must be exported before a potential termination
  # of bashrc when the shell is not interactive
  vm.RemoteCommand('sudo cat {tmp} {bashrc} | sudo tee {bashrc}'.format(
      tmp=TEMP_BASHRC,
      bashrc=BASHRC,
  ))

  # Verify that libtcmalloc is preloaded in new process
  stdout, unused_stderr = vm.RemoteCommand('echo $LD_PRELOAD')
  if 'libtcmalloc.so' not in stdout:
    raise errors.Setup.InvalidSetupError(
        'Fail to install tcmalloc. LD_PRELOAD="{}"'.format(stdout))
