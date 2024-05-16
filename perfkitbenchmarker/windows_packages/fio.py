# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing fio installation, parsing functions."""

import ntpath

WINDOWS_FIO_DIR = 'C:\\"Program Files"\\fio'
FIO_INSTALLER = 'fio_msi'
FIO_URL = 'https://github.com/axboe/fio/releases/download/fio-3.37/fio-3.37-x64.msi'


def GetFioExec():
  return ntpath.join(
      '{fio_dir}\\fio.exe --thread'.format(fio_dir=WINDOWS_FIO_DIR)
  )


def GetRemoteJobFilePath(vm):
  return '{path}\\fio.job'.format(path=vm.temp_dir)


def Install(vm):
  zip_path = ntpath.join(vm.temp_dir, FIO_INSTALLER)
  vm.DownloadFile(FIO_URL, zip_path)
  vm.RemoteCommand(f'msiexec /package  {zip_path} /quiet')
