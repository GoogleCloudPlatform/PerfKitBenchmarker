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

"""Fio package for Windows.
"""

import logging
import os

ZIP_NAME = 'fio-2.2.10-x64.zip'
ZIP_URL = 'http://www.bluestop.org/fio/releases/' + ZIP_NAME

FIO_PATH = None

FIO_RETRIES = 3


def Install(vm):
  global FIO_PATH

  """Installs the fio package on the VM."""
  zip_path = os.path.join(vm.temp_dir, ZIP_NAME)
  vm.DownloadFile(ZIP_URL, zip_path)
  vm.UnzipFile(zip_path, vm.temp_dir)

  FIO_PATH = os.path.join(vm.temp_dir, 'fio-2.2.10-x64', 'fio.exe')
  logging.info('Fio installed at %s', FIO_PATH)
