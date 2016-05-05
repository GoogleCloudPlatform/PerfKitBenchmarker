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
"""Functions for checking that required Python packages are installed."""

from collections import deque
import os

import pkg_resources

from perfkitbenchmarker import errors


# Path of the root of the current git branch.
_BRANCH_ROOT_DIR = os.path.dirname(os.path.dirname(__file__))


def _CheckRequirements(requirements_file_path):
  """Checks that all package requirements specified in a file are met.

  Args:
    requirements_file_path: string. Path to a pip requirements file.
  """
  with open(requirements_file_path, 'rb') as fp:
    requirements_to_check = [(requirements_file_path, deque(fp.readlines()))]
  try:
    while requirements_to_check:
      file_path, lines = requirements_to_check.pop()
      while lines:
        line = lines.popleft().strip()
        if line.startswith('-r'):
          requirements_to_check.append((file_path, lines))
          file_path = os.path.join(os.path.dirname(file_path), line[2:])
          with open(file_path, 'rb') as fp:
            lines = deque(fp.readlines())
        elif line:
          pkg_resources.require(line)
  except (pkg_resources.DistributionNotFound,
          pkg_resources.VersionConflict) as e:
    # In newer versions of setuptools, these exception classes have a report
    # method that provides a readable description of the error.
    report = getattr(e, 'report', None)
    err_msg = report() if report else str(e)
    raise errors.Setup.PythonPackageRequirementUnfulfilled(
        'A Python package requirement was not met while checking "{path}": '
        '{msg}{linesep}To install required packages, execute the following '
        'command:{linesep}pip install -r "{path}"{linesep}To bypass package '
        'requirement checks, run PerfKit Benchmarker with the '
        '--ignore_package_requirements flag.'.format(
            linesep=os.linesep, msg=err_msg, path=requirements_file_path))


def CheckBasicRequirements():
  """Checks that all basic package requirements are met.

  The basic requirements include packages used by modules that are imported
  regardless of the specified cloud providers. The list of required packages
  and versions is found in the requirements.txt file in the git branch's root
  directory. If such a file does not exist, then the requirements check is
  skipped.
  """
  requirements_file_path = os.path.join(_BRANCH_ROOT_DIR, 'requirements.txt')
  if os.path.isfile(requirements_file_path):
    _CheckRequirements(requirements_file_path)


def CheckProviderRequirements(provider):
  """Checks that all provider-specific requirements are met.

  The provider-specific requirements include packages used by modules that are
  imported when using a particular cloud provider. The list of required packages
  is found in the requirements-<provider>.txt file in the git branch's root
  directory. If such a file does not exist, then no additional requirements are
  necessary.

  Args:
    provider: string. Lowercase name of the cloud provider (e.g. 'gcp').
  """
  requirements_file_path = os.path.join(
      _BRANCH_ROOT_DIR, 'perfkitbenchmarker', 'providers', provider,
      'requirements.txt')
  if os.path.isfile(requirements_file_path):
    _CheckRequirements(requirements_file_path)
