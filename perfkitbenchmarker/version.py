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


def _GitVersion():
  root_dir = os.path.dirname(os.path.dirname(__file__))
  git_dir = os.path.join(root_dir, '.git')

  try:
    version = subprocess.check_output(['git', '--git-dir', git_dir,
                                       'describe', '--always'],
                                      stderr=subprocess.STDOUT)
    return version.rstrip('\n')
  except (OSError, subprocess.CalledProcessError):
    return 'unknown'


VERSION = _GitVersion()
