#!/usr/bin/env python

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
"""Bundles PerfKit Benchmarker into a zip file."""

import os
import subprocess
import sys
import zipfile


def main(argv):
  if len(argv) != 2:
    sys.exit('Usage: %s <outfile>' % argv[0])
  zip_file_path = argv[1]

  version = subprocess.check_output(('pkb.py', '--version')).rstrip()

  with zipfile.ZipFile(zip_file_path, 'w') as zip_file:
    for dir_path, _, file_names in os.walk('perfkitbenchmarker'):
      for file_name in file_names:
        if not file_name.endswith('.pyc'):
          zip_file.write(os.path.join(dir_path, file_name))
    for file_name in ('AUTHORS', 'CHANGES.md', 'CONTRIBUTING.md', 'LICENSE',
                      'README.md', 'requirements.txt'):
      zip_file.write(file_name)
    zip_file.write('pkb.py', '__main__.py')
    zip_file.writestr('perfkitbenchmarker/version.txt', version)


if __name__ == '__main__':
  main(sys.argv)
