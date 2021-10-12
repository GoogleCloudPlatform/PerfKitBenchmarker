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
"""Module allows instalation Extra Packages for Enterprise Linux repository.

See https://fedoraproject.org/wiki/EPEL for details.
"""

import logging
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types

# Location of the EPEL RPM.
_EPEL_URL = 'https://dl.fedoraproject.org/pub/epel/epel-release-latest-{}.noarch.rpm'

# Dict of vm.OS_TYPE to the yum RPM to install.  Must include all OSes that
# can install EPEL.
_EPEL_URLS = {
    os_types.CENTOS7: None,  # RPM already installed
    os_types.CENTOS8: _EPEL_URL.format(8),
    os_types.RHEL7: _EPEL_URL.format(7),
    os_types.RHEL8: _EPEL_URL.format(8),
    os_types.AMAZONLINUX2: _EPEL_URL.format(7),
}

# Additional commands to run after installing the RPM.
_EPEL_CMDS = {
    os_types.CENTOS7: 'sudo yum install -y epel-release',
    os_types.CENTOS8: 'sudo dnf config-manager --set-enabled powertools'
}

# The ids of the EPEL yum repo
_EPEL_REPO_IDS = frozenset(['epel', 'epel/x86_64', 'epel/aarch64'])


def AptInstall(vm):
  del vm
  raise NotImplementedError()


def YumInstall(vm):
  """Installs epel-release repo."""
  if vm.OS_TYPE not in _EPEL_URLS:
    raise errors.Setup.InvalidConfigurationError(
        'os_type {} not in {}'.format(vm.OS_TYPE, sorted(_EPEL_URLS)))
  if IsEpelRepoInstalled(vm):
    logging.info('EPEL repo already installed')
    return
  url = _EPEL_URLS[vm.OS_TYPE]
  if url:
    vm.InstallPackages(url)
  if vm.OS_TYPE in _EPEL_CMDS:
    vm.RemoteCommand(_EPEL_CMDS[vm.OS_TYPE])
  vm.InstallPackages('yum-utils')
  vm.RemoteCommand('sudo yum-config-manager --enable epel')
  if not IsEpelRepoInstalled(vm):
    raise ValueError('EPEL repos {} not in {}'.format(
        sorted(_EPEL_REPO_IDS), sorted(Repolist(vm))))


def IsEpelRepoInstalled(vm):
  return bool(Repolist(vm).intersection(_EPEL_REPO_IDS))


def Repolist(vm, enabled=True):
  """Returns a frozenset of the yum repos ids."""
  txt, _ = vm.RemoteCommand(
      'sudo yum repolist {}'.format('enabled' if enabled else 'all'))
  hit_repo_id = False
  repos = set()
  for line in txt.splitlines():
    if hit_repo_id:
      repo_id = line.split()[0]
      if repo_id[0] == '*':  # Repo metadata is not local, still okay to use
        repo_id = repo_id[1:]
      if repo_id != 'repolist:':
        repos.add(repo_id)
    else:
      hit_repo_id = line.startswith('repo id')
  if not repos:
    raise ValueError('Could not find repo ids in {}'.format(txt))
  return frozenset(repos)
