# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""PerfKitBenchmarker version."""

import os.path
import subprocess

import pkg_resources

import perfkitbenchmarker


_STATIC_VERSION_FILE = 'version.txt'


def _GetVersion():
  # Try to pull the version from git.
  root_dir = os.path.dirname(os.path.dirname(__file__))
  git_dir = os.path.join(root_dir, '.git')
  try:
    version = subprocess.check_output(['git', '--git-dir', git_dir,
                                       'describe', '--always'],
                                      stderr=subprocess.STDOUT)
  except (OSError, subprocess.CalledProcessError):
    # Could not get the version from git. Resort to contents of the static
    # version file.
    try:
      version = pkg_resources.resource_string(perfkitbenchmarker.__name__,
                                              _STATIC_VERSION_FILE)
    except IOError:
      # Could not determine version.
      return 'unknown'
  return version.rstrip('\n')


VERSION = _GetVersion()
