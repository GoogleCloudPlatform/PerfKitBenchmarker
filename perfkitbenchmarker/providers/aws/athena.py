# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for AWS's Athena EDW service."""

from perfkitbenchmarker import edw_service
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers


FLAGS = flags.FLAGS


class Athena(edw_service.EdwService):
  """Object representing a Athena data warehouse."""

  CLOUD = providers.AWS
  SERVICE_TYPE = 'athena'

  def _Create(self):
    """Create a Athena data warehouse."""
    raise NotImplementedError

  def _Exists(self):
    """Method to validate the existence of a Athena data warehouse.

    Returns:
      Boolean value indicating the existence of a Athena data warehouse.
    """
    raise NotImplementedError

  def _Delete(self):
    """Delete a Athena data warehouse."""
    raise NotImplementedError

  def GetMetadata(self):
    """Return a dictionary of the metadata for the Athena data warehouse."""
    basic_data = super(Athena, self).GetMetadata()
    return basic_data

  def RunCommandHelper(self):
    """Athena data warehouse specific run script command components."""
    raise NotImplementedError

  def InstallAndAuthenticateRunner(self, vm):
    """Method to perform installation and authentication of Athena query runner.

    Athena APIs are included in the default AWS cli
    https://docs.aws.amazon.com/cli/latest/reference/athena/index.html.

    Args:
      vm: Client vm on which the query will be run.
    """
    for pkg in ('aws_credentials', 'awscli'):
      vm.Install(pkg)

  def PrepareClientVm(self, vm):
    """Prepare phase to install the runtime environment on the client vm.

    Args:
      vm: Client vm on which the script will be run.
    """
    super(Athena, self).PrepareClientVm(vm)
    self.InstallAndAuthenticateRunner(vm)

  @classmethod
  def RunScriptOnClientVm(cls, vm, database, script):
    """A function to execute a script against AWS Athena Database."""
    stdout, _ = vm.RemoteCommand('python runner.py --database=%s --script=%s' %
                                 (database, script))
    return stdout
