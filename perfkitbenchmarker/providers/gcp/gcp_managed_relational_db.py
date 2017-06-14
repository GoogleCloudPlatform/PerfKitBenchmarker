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

from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import managed_relational_db
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS


class GCPManagedRelationalDb(managed_relational_db.BaseManagedRelationalDb):
  """An object representing a GCP managed relational database.

  Attributes:
    created: True if the resource has been created.
    pkb_managed: Whether the resource is managed (created and deleted) by PKB.
  """

  CLOUD = providers.GCP
  SERVICE_NAME = 'managed_relational_db'

  def GetEndpoint(self):
    pass

  def GetPort(self):
    pass

  @staticmethod
  # TODO: implement for real
  def GetLatestDatabaseVersion(database):
    return '5.6'

  def __init__(self, managed_relational_db_spec):
    super(GCPManagedRelationalDb, self).__init__(managed_relational_db_spec)
    self.spec = managed_relational_db_spec
    self.project = FLAGS.project or util.GetDefaultProject()
    self.instance_id = 'pkb-db-instance-' + FLAGS.run_uri

  def _Create(self):
    """Creates the GCP Cloud SQL instance"""
    cloudsql_specific_database_version = self._GetDatabaseVersionNameFromFlavor(
        self.spec.database,
        self.spec.database_version)
    cmd = util.GcloudCommand(self, 'sql', 'instances', 'create',
                             self.instance_id)
    cmd.flags['project'] = self.project
    cmd.flags['database-version'] = cloudsql_specific_database_version

    cmd.Issue()

  def _Delete(self):
    """Deletes the underlying resource.

    Implementations of this method should be idempotent since it may
    be called multiple times, even if the resource has already been
    deleted.
    """
    cmd = util.GcloudCommand(self, 'sql', 'instances', 'delete',
                             self.instance_id)
    cmd.Issue()

  def _Exists(self):
    """Returns true if the underlying resource exists.

    Supplying this method is optional. If it is not implemented then the
    default is to assume success when _Create and _Delete do not raise
    exceptions.
    """
    cmd = util.GcloudCommand(self, 'sql', 'instances', 'describe',
                             self.instance_id)
    _, _, retcode = cmd.Issue()
    return retcode == 0

  def _IsReady(self):
    """Return true if the underlying resource is ready.

    Supplying this method is optional.  Use it when a resource can exist
    without being ready.  If the subclass does not implement
    it then it just returns true.

    Returns:
      True if the resource was ready in time, False if the wait timed out.
    """
    return True

  def _PostCreate(self):
    """Method that will be called once after _CreateReource is called.

    Supplying this method is optional. If it is supplied, it will be called
    once, after the resource is confirmed to exist. It is intended to allow
    data about the resource to be collected or for the resource to be tagged.
    """
    pass

  def _CreateDependencies(self):
    """Method that will be called once before _CreateResource() is called.

    Supplying this method is optional. It is intended to allow additional
    flexibility in creating resource dependencies separately from _Create().
    """
    pass

  def _DeleteDependencies(self):
    """Method that will be called once after _DeleteResource() is called.

    Supplying this method is optional. It is intended to allow additional
    flexibility in deleting resource dependencies separately from _Delete().
    """
    pass

  @staticmethod
  def _GetDatabaseVersionNameFromFlavor(flavor, version):
    if flavor == 'mysql':
      if version == '5.6':
        return 'MYSQL_5_6'
    raise NotImplementedError('GCP managed databases only support MySQL 5.6')
