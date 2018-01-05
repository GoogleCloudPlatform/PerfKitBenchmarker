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
"""Module containing MXNet CNN installation and cleanup functions."""
from perfkitbenchmarker import flags
FLAGS = flags.FLAGS
MXNET_GIT = 'https://github.com/apache/incubator-mxnet.git'

flags.DEFINE_string('mxnet_commit_hash',
                    '2700ddbbeef212879802f7f0c0812192ec5c2b77',
                    'git commit hash of desired mxnet commit.')


def Install(vm):
  """Installs MXNet on the VM."""
  vm.InstallPackages('git')
  vm.RemoteCommand('git clone %s' % MXNET_GIT, should_log=True)
  vm.RemoteCommand('cd incubator-mxnet && git checkout %s' %
                   FLAGS.mxnet_commit_hash)


def Uninstall(vm):
  """Uninstalls MXNet on the VM."""
  vm.RemoteCommand('rm -rf tpu-demos', should_log=True)


def GetCommit(vm):
  stdout, _ = vm.RemoteCommand('cd incubator-mxnet && git rev-parse HEAD',
                               should_log=True)
  return stdout
