# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing apache2 installation and setup functions."""

import tempfile
from perfkitbenchmarker import virtual_machine


def AptInstall(vm: virtual_machine.VirtualMachine) -> None:
  """Installs the apache2 package on the VM."""
  vm.Install('apache2_utils')
  vm.InstallPackages('apache2')


def SetupServer(vm: virtual_machine.VirtualMachine, content_bytes: int) -> None:
  """Sets up the apache server on the VM.

  Args:
    vm: The VM that will have the apache server set up.
    content_bytes: The number of bytes the content that the apache server will
    serve will be.
  """
  # Set up access to vm/var/www
  vm.RemoteCommand('sudo chown -R $USER:$USER /var/www')

  # Set up basic HTML webpage
  with tempfile.NamedTemporaryFile() as tmp:
    tmp.write(f'''
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>My test page</title>
      </head>
      <body>
        <img src="cat.png" alt="My test image">
        <p> {'a' * content_bytes} </p>
      </body>
    </html>
    '''.encode())
    vm.RemoteCopy(tmp.name, '/var/www/html/index.html', True)

  # Set up read access to index.html
  vm.RemoteCommand('sudo chmod 644 /var/www/html/index.html')

  # Download sample image to serve
  vm.RemoteCommand(
      'wget --output-document=/var/www/html/cat.png https://http.cat/100'
  )


def StartServer(vm: virtual_machine.VirtualMachine) -> None:
  """Starts the apache server on the VM."""
  vm.RemoteCommand('sudo systemctl reload apache2')
