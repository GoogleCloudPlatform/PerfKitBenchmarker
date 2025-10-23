# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Classes for GCP Firestore databases."""

import json
import logging
from typing import Any

from absl import flags
from perfkitbenchmarker import non_relational_db
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS

_NAME = flags.DEFINE_string(
    'gcp_firestore_database_id',
    None,
    'Firestore instance name. If not specified, new instance '
    'will be created and deleted on the fly. If specified, '
    'the instance is considered user managed and will not '
    'created/deleted by PKB.',
)
_LOCATION = flags.DEFINE_string(
    'gcp_firestore_location',
    None,
    'Location of the firestore database. See'
    ' https://firebase.google.com/docs/firestore/locations.',
)

_DEFAULT_LOCATION = 'us-central1'
_DEFAULT_TYPE = 'firestore-native'
_DEFAULT_EDITION = 'enterprise'


class FirestoreSpec(non_relational_db.BaseNonRelationalDbSpec):
  """Configurable options of a Firestore database."""

  SERVICE_TYPE = non_relational_db.FIRESTORE

  database_id: str
  location: str

  def __init__(self, component_full_name, flag_values, **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes / constructor args for each configurable option."""
    result = super()._GetOptionDecoderConstructions()
    none_ok = {'default': None, 'none_ok': True}
    result.update({
        'database_id': (option_decoders.StringDecoder, none_ok),
        'location': (option_decoders.StringDecoder, none_ok),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values) -> None:
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. May be
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.
    """
    super()._ApplyFlags(config_values, flag_values)
    option_name_from_flag = {
        'gcp_firestore_database_id': 'database_id',
        'gcp_firestore_location': 'location',
    }
    for flag_name, option_name in option_name_from_flag.items():
      if flag_values[flag_name].present:
        config_values[option_name] = flag_values[flag_name].value

  def __repr__(self) -> str:
    return str(self.__dict__)


class Firestore(non_relational_db.BaseManagedMongoDb):
  """Object representing a GCP Firestore database.

  See https://cloud.google.com/firestore/mongodb-compatibility/docs/overview.

  Attributes:
    id: Database ID.
    location: Location of the database.
    project: Project of the database. VMs in the same project can access the
      database with the https://www.googleapis.com/auth/firestore scope.
  """

  SERVICE_TYPE = non_relational_db.FIRESTORE

  def __init__(self, database_id: str | None, location: str | None, **kwargs):
    super().__init__(**kwargs)
    self.database_id: str = database_id or f'pkb-firestore-{FLAGS.run_uri}'
    self.location: str = location or self._GetDefaultLocation()
    self.project: str = FLAGS.project or util.GetDefaultProject()
    self.edition: str = _DEFAULT_EDITION
    self.type: str = _DEFAULT_TYPE
    self.uid: str = None
    self.port: int = 443
    self.tls_enabled: bool = True

  @classmethod
  def FromSpec(cls, spec: FirestoreSpec) -> 'Firestore':
    return cls(
        database_id=spec.database_id,
        location=spec.location,
    )

  def _GetDefaultLocation(self) -> str:
    """Gets the config that corresponds the region used for the test."""
    return (
        util.GetRegionFromZone(FLAGS.zone[0])
        if FLAGS.zone
        else _DEFAULT_LOCATION
    )

  def _Create(self):
    """Creates the database."""
    cmd = util.GcloudCommand(self, 'firestore', 'databases', 'create')
    cmd.flags['database'] = self.database_id
    cmd.flags['location'] = self.location
    cmd.flags['type'] = self.type
    cmd.flags['edition'] = self.edition
    cmd.Issue(raise_on_failure=False)

  def _Delete(self):
    """Deletes the database."""
    cmd = util.GcloudCommand(self, 'firestore', 'databases', 'delete')
    cmd.flags['database'] = self.database_id
    cmd.Issue(raise_on_failure=False)

  def _DescribeDatabase(self) -> dict[str, Any]:
    cmd = util.GcloudCommand(self, 'firestore', 'databases', 'describe')
    cmd.flags['database'] = self.database_id
    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error(
          'Describing instance %s failed: %s', self.database_id, stderr
      )
      return {}
    return json.loads(stdout)

  def _Exists(self) -> bool:
    """Returns true if the database exists."""
    database = self._DescribeDatabase()
    if not database:
      return False
    return 'createTime' in database

  def _PostCreate(self) -> None:
    database = self._DescribeDatabase()
    self.uid = database['uid']
    self.endpoint = self.uid + '.' + self.location + '.firestore.goog'

  def GetConnectionString(self) -> str:
    return (
        f'mongodb://{self.endpoint}:{self.port}/{self.database_id}'
        '?loadBalanced=true'
        '&authMechanism=MONGODB-OIDC'
        '&authMechanismProperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:FIRESTORE'
        f'&tls={str(self.tls_enabled).lower()}'
        '&retryWrites=false'
    )

  def GetResourceMetadata(self) -> dict[Any, Any]:
    metadata = super().GetResourceMetadata()
    metadata.update({
        'firestore_database_id': self.database_id,
        'firestore_location': self.location,
        'firestore_edition': self.edition,
        'firestore_type': self.type,
    })
    return metadata
