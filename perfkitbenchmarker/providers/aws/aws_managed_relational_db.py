from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import managed_relational_db
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util
from perfkitbenchmarker.providers.aws import aws_disk

FLAGS = flags.FLAGS

class AwsManagedRelationalDb(managed_relational_db.BaseManagedRelationalDb):
  """An object representing an AWS managed relational database.

  Attributes:
    created: True if the resource has been created.
    pkb_managed: Whether the resource is managed (created and deleted) by PKB.
  """

  CLOUD = providers.AWS
  SERVICE_NAME = 'managed_relational_db'

  @staticmethod
  # TODO: implement for real
  def GetLatestDatabaseVersion(database):
    return '5.6'

  def __init__(self, managed_relational_db_spec):
    super(AwsManagedRelationalDb, self).__init__(managed_relational_db_spec)
    self.spec = managed_relational_db_spec
    self.instance_id = 'pkb-db-instance-' + FLAGS.run_uri

  def _Create(self):
    """Creates the AWS RDS instance"""
    database_version = (
        self._GetDatabaseVersionNameFromFlavor(
          self.spec.database,
          self.spec.database_version))
    cmd = util.AWS_PREFIX + [
        'rds',
        'create-db-instance',
        '--db-instance-identifier %s' % self.instance_id,
        '--engine %s' % self.spec.database,
        '--master-username %s' % self.spec.db_username,
        '--master-user-password %s' % self.spec.db_password,
        '--allocated-storage %s' % self.spec.disk_spec.disk_size,
        '--storage-type %s' % self.spec.disk_spec.disk_type,
        '--db-instance-class %s' % self.spec.vm_spec.machine_type
    ]

    if self.spec.disk_spec.disk_type == aws_disk.IO1:
      cmd.append('--iops %s' % self.spec.disk_spec.iops)

    vm_util.IssueCommand(cmd)

  def _Delete(self):
    """Deletes the underlying resource.

    Implementations of this method should be idempotent since it may
    be called multiple times, even if the resource has already been
    deleted.
    """
    cmd = util.AWS_PREFIX + [
        'rds',
        'delete-db-instance',
        'db-instance-identifier%s ' % self.instance_id
    ]
    vm_util.IssueCommand(cmd)

  def _Exists(self):
    """Returns true if the underlying resource exists.

    Supplying this method is optional. If it is not implemented then the
    default is to assume success when _Create and _Delete do not raise
    exceptions.
    """
    cmd = util.AWS_PREFIX + [
        'rds',
        'describe-db-instances'
        '--db-instance-identifier %s' % self.instance_id,
        '--region %s' % self.region
    ]
    _, _, retcode = vm_util.IssueCommand()
    return retcode == 0

  def _IsReady(self):
    """Return true if the underlying resource is ready.

    Supplying this method is optional.  Use it when a resource can exist
    without being ready.  If the subclass does not implement
    it then it just returns true.

    Returns:
      True if the resource was ready in time, False if the wait timed out.
    """
    cmd = util.AWS_PREFIX + [
        'rds',
        'describe-db-instances'
        '--db-instance-identifier %s' % self.instance_id,
        '--region %s' % self.region
    ]
    stdout, _ = vm_util.IssueCommand()
    return json.loads(stdout).DBInstances[0].DBInstanceStatus == 'available'

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
