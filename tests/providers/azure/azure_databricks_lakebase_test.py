# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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

import inspect
import os
from typing import Any
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker.providers.azure import azure_databricks_lakebase
from tests import pkb_common_test_case
import requests

FLAGS = flags.FLAGS

# Clearly dummy values for testing. None of these are real credentials.
_DUMMY_DBX_HOST = 'https://dummy-workspace.azuredatabricks.net'
_DUMMY_DBX_TOKEN = 'dummy-databricks-token'
_DUMMY_OAUTH_TOKEN = 'dummy-oauth-token'
_DUMMY_DB_USERNAME = 'dummy-user'
_DUMMY_DB_PASSWORD = 'dummy-password'
_DUMMY_DATABRICKS_USER = 'dummy-databricks-user'

_DEFAULT_YAML = inspect.cleandoc(f"""
    sysbench:
      relational_db:
        engine: lakebase-postgres
        engine_version: '17'
        database_username: {_DUMMY_DB_USERNAME}
        database_password: {_DUMMY_DB_PASSWORD}
        db_spec: *default_dual_core
        db_disk_spec: *default_500_gb
        vm_groups:
          clients:
            vm_spec: *default_dual_core
            disk_spec: *default_500_gb
""")


def _MakeMockResponse(
    status_code: int = 200,
    json_data: dict[str, Any] | None = None,
    text: str = '',
) -> mock.Mock:
  """Creates a mock requests.Response with the given status and payload."""
  response = mock.create_autospec(requests.Response, instance=True)
  response.status_code = status_code
  response.ok = 200 <= status_code < 300
  if json_data is not None:
    response.text = text or '{}'
    response.json.return_value = json_data
  else:
    response.text = text
    response.json.return_value = {}
  return response


class AzureDatabricksLakebaseTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = 'abc12345'
    FLAGS.cloud = provider_info.AZURE
    FLAGS['db_engine'].parse(sql_engine_utils.LAKEBASE_POSTGRES)

    temp_home = self.create_tempdir()
    self.enter_context(
        mock.patch.dict(os.environ, {'HOME': temp_home.full_path})
    )
    self.cfg_path = os.path.join(temp_home.full_path, '.databrickscfg')
    with open(self.cfg_path, 'w') as cfg_file:
      cfg_file.write(
          f'[azure]\nhost = {_DUMMY_DBX_HOST}\ntoken = {_DUMMY_DBX_TOKEN}\n'
      )

  def _CreateDbFromYaml(
      self,
      yaml_str: str = _DEFAULT_YAML,
      benchmark_name: str = 'sysbench',
  ) -> azure_databricks_lakebase.AzureDatabricksLakebase:
    bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=yaml_str, benchmark_name=benchmark_name
    )
    bm_spec.ConstructRelationalDb()
    db = bm_spec.relational_db
    self.assertIsInstance(db, azure_databricks_lakebase.AzureDatabricksLakebase)
    return db

  def testInitializationDefaultsFromYaml(self):
    yaml_str = inspect.cleandoc(f"""
        sysbench:
          relational_db:
            engine: lakebase-postgres
            database_username: {_DUMMY_DB_USERNAME}
            database_password: {_DUMMY_DB_PASSWORD}
            db_spec: *default_dual_core
            db_disk_spec: *default_500_gb
            vm_groups:
              clients:
                vm_spec: *default_dual_core
                disk_spec: *default_500_gb
    """)

    db = self._CreateDbFromYaml(yaml_str)
    metadata = db.GetResourceMetadata()

    with self.subTest(name='provider_and_engine'):
      db_class = relational_db.GetRelationalDbClass(
          provider_info.AZURE, True, sql_engine_utils.LAKEBASE_POSTGRES
      )
      self.assertIs(db_class, azure_databricks_lakebase.AzureDatabricksLakebase)
      self.assertEqual(
          sql_engine_utils.GetDbEngineType(sql_engine_utils.LAKEBASE_POSTGRES),
          sql_engine_utils.POSTGRES,
      )
      self.assertEqual(
          db.GetDefaultEngineVersion(sql_engine_utils.LAKEBASE_POSTGRES), '17'
      )
    with self.subTest(name='default_attributes'):
      self.assertEqual(db.project_id, 'pkb-abc12345')
      self.assertEqual(db.min_cu, 2.0)
      self.assertEqual(db.max_cu, 2.0)
      self.assertFalse(db.spec.high_availability)
    with self.subTest(name='discards_vm_and_disk_specs'):
      self.assertIsNone(db.spec.db_spec)
      self.assertIsNone(db.spec.db_disk_spec)
      self.assertIsNone(metadata['disk_size'])
      self.assertIsNone(metadata['disk_type'])
    with self.subTest(name='resource_metadata'):
      self.assertEqual(metadata['lakebase_project_id'], 'pkb-abc12345')
      self.assertEqual(metadata['instance_id'], 'pkb-abc12345')
      self.assertEqual(metadata['lakebase_min_cu'], 2.0)
      self.assertEqual(metadata['lakebase_max_cu'], 2.0)
      self.assertEqual(metadata['compute_units'], 2.0)
      self.assertEqual(metadata['memory'], 4096)
      self.assertEqual(metadata['machine_type'], '2CU')
      self.assertEqual(metadata['endpoint_group_size'], 1)
      self.assertEqual(metadata['zone'], 'eastus-1')
      self.assertEqual(metadata['engine'], sql_engine_utils.LAKEBASE_POSTGRES)
      self.assertEqual(metadata['engine_version'], '17')
      self.assertFalse(metadata['high_availability'])
      self.assertTrue(metadata['use_managed_db'])
      self.assertTrue(metadata['backup_enabled'])
      self.assertNotIn('cpus', metadata)

  @flagsaver.flagsaver
  def testInitializationFromFlags(self):
    FLAGS['lakebase_project_id'].parse('custom-proj')
    FLAGS['lakebase_endpoint_id'].parse('custom-ep')
    FLAGS['lakebase_min_cu'].parse(16.0)
    FLAGS['lakebase_max_cu'].parse(32.0)
    FLAGS['db_high_availability'].parse(True)
    FLAGS['db_zone'].parse(['westus2'])
    FLAGS['db_disk_size'].parse(500)
    FLAGS['db_machine_type'].parse('Standard_D4s_v3')

    db = self._CreateDbFromYaml()
    metadata = db.GetResourceMetadata()

    with self.subTest(name='lakebase_flags'):
      self.assertEqual(db.project_id, 'custom-proj')
      self.assertEqual(
          db.endpoint_name,
          'projects/custom-proj/branches/production/endpoints/custom-ep',
      )
      self.assertEqual(db.min_cu, 16.0)
      self.assertEqual(db.max_cu, 32.0)
      self.assertTrue(db.spec.high_availability)
    with self.subTest(name='metadata_and_discarded_server_flags'):
      self.assertIsNone(db.spec.db_spec)
      self.assertIsNone(db.spec.db_disk_spec)
      self.assertEqual(metadata['zone'], 'westus2')
      self.assertEqual(metadata['lakebase_project_id'], 'custom-proj')
      self.assertEqual(metadata['instance_id'], 'custom-proj')
      self.assertEqual(metadata['lakebase_min_cu'], 16.0)
      self.assertEqual(metadata['lakebase_max_cu'], 32.0)
      self.assertEqual(metadata['compute_units'], 32.0)
      self.assertEqual(metadata['machine_type'], '16-32CU')
      self.assertEqual(metadata['memory'], 65536)
      self.assertEqual(metadata['endpoint_group_size'], 2)
      self.assertTrue(metadata['high_availability'])

  @parameterized.named_parameters(
      ('DefaultCu', None, None, 2.0, 2.0, '2CU'),
      ('FromExplicitMinOnly', 16.0, None, 16.0, 16.0, '16CU'),
      ('FromExplicitMaxOnly', None, 24.0, 24.0, 24.0, '24CU'),
      ('FromExplicitRange', 8.0, 16.0, 8.0, 16.0, '8-16CU'),
      ('FractionalMinAndMax', 0.5, 0.5, 0.5, 0.5, '0.5CU'),
      ('FractionalMinRange', 0.5, 4.0, 0.5, 4.0, '0.5-4CU'),
  )
  @flagsaver.flagsaver
  def testResolveComputeUnits(
      self,
      min_cu_flag: float | None,
      max_cu_flag: float | None,
      expected_min: float,
      expected_max: float,
      expected_machine_type: str,
  ):
    if min_cu_flag is not None:
      FLAGS['lakebase_min_cu'].parse(min_cu_flag)
    if max_cu_flag is not None:
      FLAGS['lakebase_max_cu'].parse(max_cu_flag)

    db = self._CreateDbFromYaml()

    self.assertEqual(db.min_cu, expected_min)
    self.assertEqual(db.max_cu, expected_max)
    metadata = db.GetResourceMetadata()
    self.assertEqual(metadata['lakebase_min_cu'], expected_min)
    self.assertEqual(metadata['lakebase_max_cu'], expected_max)
    self.assertEqual(metadata['compute_units'], expected_max)
    self.assertEqual(
        metadata['memory'],
        int(expected_max * azure_databricks_lakebase.MEMORY_MIB_PER_CU),
    )
    self.assertEqual(metadata['machine_type'], expected_machine_type)

  def testGetResourceMetadataDefaults(self):
    db = self._CreateDbFromYaml()
    metadata = db.GetResourceMetadata()

    self.assertEqual(
        metadata,
        {
            'zone': 'eastus-1',
            'disk_type': None,
            'disk_size': None,
            'db_tier': None,
            'engine': sql_engine_utils.LAKEBASE_POSTGRES,
            'high_availability': False,
            'backup_enabled': True,
            'engine_version': '17',
            'client_vm_zone': 'eastus-1',
            'use_managed_db': True,
            'instance_id': 'pkb-abc12345',
            'client_vm_disk_type': 'PremiumV2_LRS',
            'client_vm_disk_size': 500,
            'client_vm_machine_type': 'Standard_D2s_v6',
            'endpoint_group_size': 1,
            'lakebase_project_id': 'pkb-abc12345',
            'lakebase_min_cu': 2.0,
            'lakebase_max_cu': 2.0,
            'compute_units': 2.0,
            'memory': 4096,
            'machine_type': '2CU',
        },
    )

  @flagsaver.flagsaver
  def testGetResourceMetadataWithCustomOptions(self):
    FLAGS['lakebase_project_id'].parse('lakebase-custom-project')
    FLAGS['lakebase_min_cu'].parse(4.0)
    FLAGS['lakebase_max_cu'].parse(16.0)
    FLAGS['db_high_availability'].parse(True)
    FLAGS['db_zone'].parse(['centralus'])

    db = self._CreateDbFromYaml()
    metadata = db.GetResourceMetadata()

    with self.subTest(name='project_and_instance_id'):
      self.assertEqual(
          metadata['lakebase_project_id'], 'lakebase-custom-project'
      )
      self.assertEqual(metadata['instance_id'], 'lakebase-custom-project')
    with self.subTest(name='compute_and_memory'):
      self.assertEqual(metadata['lakebase_min_cu'], 4.0)
      self.assertEqual(metadata['lakebase_max_cu'], 16.0)
      self.assertEqual(metadata['compute_units'], 16.0)
      self.assertEqual(metadata['memory'], 32768)
      self.assertEqual(metadata['machine_type'], '4-16CU')
    with self.subTest(name='high_availability_and_endpoint_group'):
      self.assertEqual(metadata['endpoint_group_size'], 2)
      self.assertTrue(metadata['high_availability'])
    with self.subTest(name='zone'):
      self.assertEqual(metadata['zone'], 'centralus')

  @parameterized.named_parameters(
      (
          'FromExplicitDbZone',
          ['centralus', 'eastus2'],
          'eastus-1',
          'centralus',
      ),
      (
          'FallbackToClientVmZone',
          None,
          'northcentralus',
          'northcentralus',
      ),
      (
          'NoneWhenNeitherZoneSpecified',
          None,
          None,
          None,
      ),
  )
  @flagsaver.flagsaver
  def testGetResourceMetadataZoneResolution(
      self,
      db_zone_flag: list[str] | None,
      client_zone: str | None,
      expected_zone: str | None,
  ):
    if db_zone_flag is not None:
      FLAGS['db_zone'].parse(db_zone_flag)
    if client_zone is not None:
      client_vm_spec = f"""
                vm_spec:
                  Azure:
                    machine_type: Standard_D2s_v6
                    zone: {client_zone}
      """
    else:
      client_vm_spec = """
                vm_spec:
                  Azure:
                    machine_type: Standard_D2s_v6
      """
    yaml_str = inspect.cleandoc(f"""
        sysbench:
          relational_db:
            engine: lakebase-postgres
            db_spec: *default_dual_core
            db_disk_spec: *default_500_gb
            vm_groups:
              clients:
{client_vm_spec}
                disk_spec: *default_500_gb
    """)
    db = self._CreateDbFromYaml(yaml_str)
    metadata = db.GetResourceMetadata()
    self.assertEqual(metadata['zone'], expected_zone)
    self.assertEqual(metadata.get('client_vm_zone'), client_zone)

  @parameterized.named_parameters(
      ('SingleNode', False, 1),
      ('HighAvailability', True, 2),
  )
  @flagsaver.flagsaver
  def testGetResourceMetadataEndpointGroupSize(
      self,
      high_availability: bool,
      expected_group_size: int,
  ):
    FLAGS['db_high_availability'].parse(high_availability)
    db = self._CreateDbFromYaml()
    metadata = db.GetResourceMetadata()
    self.assertEqual(metadata['endpoint_group_size'], expected_group_size)
    self.assertEqual(metadata['high_availability'], high_availability)

  @parameterized.named_parameters(
      ('NegativeCu', -4.0, 8.0),
      ('MaxLessThanMin', 16.0, 8.0),
      ('SpreadExceeds16', 8.0, 32.0),
      ('ExceedsMax64', 64.0, 72.0),
  )
  @flagsaver.flagsaver
  def testInvalidComputeUnitsRaisesError(self, min_cu: float, max_cu: float):
    FLAGS['lakebase_min_cu'].parse(min_cu)
    FLAGS['lakebase_max_cu'].parse(max_cu)

    with self.assertRaises(errors.Config.InvalidValue):
      self._CreateDbFromYaml()

  @parameterized.named_parameters(
      (
          'NonHa',
          False,
          {
              'autoscaling_limit_min_cu': 2.0,
              'autoscaling_limit_max_cu': 2.0,
              'no_suspension': True,
          },
      ),
      (
          'Ha',
          True,
          {
              'autoscaling_limit_min_cu': 2.0,
              'autoscaling_limit_max_cu': 2.0,
              'no_suspension': True,
              'group': {
                  'min': 2,
                  'max': 2,
                  'enable_readable_secondaries': True,
              },
          },
      ),
  )
  @flagsaver.flagsaver
  def testCreateProjectAndInitialEndpointSpec(
      self,
      high_availability: bool,
      expected_initial_endpoint_spec: dict[str, Any],
  ):
    FLAGS['db_high_availability'].parse(high_availability)
    db = self._CreateDbFromYaml()
    mock_request = self.enter_context(
        mock.patch.object(requests, 'request', autospec=True)
    )
    mock_request.side_effect = [
        _MakeMockResponse(
            200,
            {
                'name': 'projects/pkb-abc12345/operations/op-1',
                'done': False,
            },
        ),
        _MakeMockResponse(
            200,
            {
                'name': 'projects/pkb-abc12345/operations/op-1',
                'done': True,
                'response': {'spec': {'enable_pg_native_login': False}},
            },
        ),
        _MakeMockResponse(200, {'done': True}),
    ]

    db._Create()

    self.assertLen(mock_request.call_args_list, 3)
    create_kwargs = mock_request.call_args_list[0].kwargs
    with self.subTest(name='create_project_request'):
      self.assertEqual(create_kwargs['params'], {'project_id': 'pkb-abc12345'})
      project_spec = create_kwargs['json']['spec']
      self.assertEqual(project_spec['pg_version'], 17)
      self.assertTrue(project_spec['enable_pg_native_login'])
      self.assertEqual(
          project_spec['default_endpoint_settings'],
          {
              'autoscaling_limit_min_cu': 2.0,
              'autoscaling_limit_max_cu': 2.0,
              'no_suspension': True,
          },
      )
    with self.subTest(name='initial_endpoint_spec'):
      self.assertEqual(
          create_kwargs['json']['initial_endpoint_spec'],
          expected_initial_endpoint_spec,
      )
    with self.subTest(name='patches_native_login_when_false'):
      patch_kwargs = mock_request.call_args_list[2].kwargs
      self.assertEqual(
          patch_kwargs['params'], {'update_mask': 'spec.enable_pg_native_login'}
      )
      self.assertEqual(
          patch_kwargs['json'],
          {
              'name': 'projects/pkb-abc12345',
              'spec': {'enable_pg_native_login': True},
          },
      )

  @parameterized.named_parameters(
      ('Active', 200, {'name': 'projects/pkb-abc12345'}, True),
      (
          'SoftDeleted',
          200,
          {
              'name': 'projects/pkb-abc12345',
              'delete_time': '2026-09-28T18:23:07Z',
              'purge_time': '2026-10-05T18:23:07Z',
          },
          False,
      ),
      ('NotFound', 404, {'error_code': 'NOT_FOUND'}, False),
  )
  def testExists(
      self, status_code: int, project_payload: dict[str, Any], expected: bool
  ):
    db = self._CreateDbFromYaml()
    self.enter_context(
        mock.patch.object(
            requests,
            'request',
            autospec=True,
            return_value=_MakeMockResponse(status_code, project_payload),
        )
    )

    self.assertEqual(db._Exists(), expected)

  def testRequestUsesDatabricksConfigAndRedactsSecrets(self):
    db = self._CreateDbFromYaml()
    mock_request = self.enter_context(
        mock.patch.object(
            requests,
            'request',
            autospec=True,
            return_value=_MakeMockResponse(
                200,
                {
                    'token': _DUMMY_OAUTH_TOKEN,
                    'expire_time': '2026-01-01T00:00:00Z',
                },
            ),
        )
    )

    with self.assertLogs(level='INFO') as logs:
      response_data = db._Request(
          'post', '/api/2.0/postgres/credentials', json_body={'endpoint': 'e1'}
      )

    self.assertEqual(
        response_data,
        {'token': _DUMMY_OAUTH_TOKEN, 'expire_time': '2026-01-01T00:00:00Z'},
    )
    request_kwargs = mock_request.call_args.kwargs
    self.assertEqual(
        request_kwargs['url'],
        f'{_DUMMY_DBX_HOST}/api/2.0/postgres/credentials',
    )
    self.assertEqual(
        request_kwargs['headers']['Authorization'],
        f'Bearer {_DUMMY_DBX_TOKEN}',
    )
    self.assertIsNotNone(request_kwargs['auth'])
    log_output = '\n'.join(logs.output)
    self.assertIn('"token": "<redacted>"', log_output)
    self.assertNotIn(_DUMMY_OAUTH_TOKEN, log_output)
    self.assertNotIn(_DUMMY_DBX_TOKEN, log_output)

  @parameterized.named_parameters(
      ('MissingFile', None),
      (
          'MissingProfile',
          f'[gcp]\nhost = {_DUMMY_DBX_HOST}\ntoken = {_DUMMY_DBX_TOKEN}\n',
      ),
      ('MissingToken', f'[azure]\nhost = {_DUMMY_DBX_HOST}\n'),
  )
  def testRequestInvalidAuthConfigRaisesError(self, cfg_contents: str | None):
    db = self._CreateDbFromYaml()
    empty_home = self.create_tempdir()
    if cfg_contents is not None:
      cfg_file_path = os.path.join(empty_home.full_path, '.databrickscfg')
      with open(cfg_file_path, 'w') as cfg_file:
        cfg_file.write(cfg_contents)

    with mock.patch.dict(os.environ, {'HOME': empty_home.full_path}):
      with self.assertRaises(errors.Config.InvalidValue):
        db._LoadDatabricksAuth()

  def testRequestHttpErrorRaisesCreationError(self):
    db = self._CreateDbFromYaml()
    mock_response = _MakeMockResponse(502, text='Bad Gateway')
    mock_response.json.side_effect = ValueError('not JSON')
    self.enter_context(
        mock.patch.object(
            requests, 'request', autospec=True, return_value=mock_response
        )
    )

    with self.assertRaisesRegex(errors.Resource.CreationError, '502'):
      db._Request('GET', '/api/2.0/postgres/projects/p')

  def testWaitForOperationPollsUntilDone(self):
    db = self._CreateDbFromYaml()
    self.enter_context(
        mock.patch.object(
            azure_databricks_lakebase.time, 'sleep', autospec=True
        )
    )
    mock_request = self.enter_context(
        mock.patch.object(requests, 'request', autospec=True)
    )
    mock_request.side_effect = [
        _MakeMockResponse(
            200,
            {'name': 'projects/pkb-abc12345/operations/op-1', 'done': False},
        ),
        _MakeMockResponse(
            200,
            {'name': 'projects/pkb-abc12345/operations/op-1', 'done': True},
        ),
    ]

    result = db._WaitForOperation('projects/pkb-abc12345/operations/op-1')

    self.assertTrue(result['done'])
    self.assertEqual(mock_request.call_count, 2)

  def testWaitForOperationFailureRaisesError(self):
    db = self._CreateDbFromYaml()
    self.enter_context(
        mock.patch.object(
            requests,
            'request',
            autospec=True,
            return_value=_MakeMockResponse(
                200,
                {
                    'name': 'projects/pkb-abc12345/operations/op-err',
                    'error': {'message': 'Quota exceeded'},
                },
            ),
        )
    )

    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'Quota exceeded'
    ):
      db._WaitForOperation('projects/pkb-abc12345/operations/op-err')

  def testGetCurrentDatabricksUserSuccess(self):
    db = self._CreateDbFromYaml()
    self.enter_context(
        mock.patch.object(
            db,
            '_Request',
            autospec=True,
            return_value={'userName': _DUMMY_DATABRICKS_USER},
        )
    )
    self.assertEqual(db._GetCurrentDatabricksUser(), _DUMMY_DATABRICKS_USER)

  def testGetCurrentDatabricksUserApplicationIdFallback(self):
    db = self._CreateDbFromYaml()
    self.enter_context(
        mock.patch.object(
            db,
            '_Request',
            autospec=True,
            return_value={'applicationId': 'dummy-app-id'},
        )
    )
    self.assertEqual(db._GetCurrentDatabricksUser(), 'dummy-app-id')

  def testGetCurrentDatabricksUserMissingRaisesError(self):
    db = self._CreateDbFromYaml()
    self.enter_context(
        mock.patch.object(db, '_Request', autospec=True, return_value={})
    )
    with self.assertRaisesRegex(
        errors.Resource.CreationError,
        'Unable to determine current Databricks user',
    ):
      db._GetCurrentDatabricksUser()

  def testGenerateDatabaseCredentialSuccess(self):
    db = self._CreateDbFromYaml()
    db.endpoint_name = (
        'projects/pkb-abc12345/branches/production/endpoints/primary'
    )
    mock_request = self.enter_context(
        mock.patch.object(
            db,
            '_Request',
            autospec=True,
            return_value={'token': _DUMMY_OAUTH_TOKEN},
        )
    )
    token = db._GenerateDatabaseCredential()
    self.assertEqual(token, _DUMMY_OAUTH_TOKEN)
    mock_request.assert_called_once_with(
        'POST',
        '/api/2.0/postgres/credentials',
        json_body={'endpoint': db.endpoint_name},
    )

  def testGenerateDatabaseCredentialMissingTokenRaisesError(self):
    db = self._CreateDbFromYaml()
    db.endpoint_name = (
        'projects/pkb-abc12345/branches/production/endpoints/primary'
    )
    self.enter_context(
        mock.patch.object(db, '_Request', autospec=True, return_value={})
    )
    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'did not contain a token'
    ):
      db._GenerateDatabaseCredential()

  def testConfigureNativeDatabaseRole(self):
    db = self._CreateDbFromYaml()
    db.endpoint = 'dummy-host.postgres.database.azure.com'
    db.endpoint_name = (
        'projects/pkb-abc12345/branches/production/endpoints/primary'
    )
    mock_vm = mock.Mock()
    mock_vm.RemoteCommandWithReturnCode.return_value = ('', '', 0)
    db.client_vm = mock_vm

    self.enter_context(
        mock.patch.object(
            db,
            '_GetCurrentDatabricksUser',
            return_value=_DUMMY_DATABRICKS_USER,
        )
    )
    self.enter_context(
        mock.patch.object(
            db,
            '_GenerateDatabaseCredential',
            return_value=_DUMMY_OAUTH_TOKEN,
        )
    )

    db._ConfigureNativeDatabaseRole()

    mock_vm.Install.assert_called_once_with('postgres_client')
    self.assertEqual(mock_vm.RemoteCommandWithReturnCode.call_count, 2)
    role_cmd, _ = mock_vm.RemoteCommandWithReturnCode.call_args_list[0]
    db_cmd, _ = mock_vm.RemoteCommandWithReturnCode.call_args_list[1]
    self.assertIn(_DUMMY_DB_USERNAME, role_cmd[0])
    self.assertIn(_DUMMY_DB_PASSWORD, role_cmd[0])
    self.assertIn(_DUMMY_OAUTH_TOKEN, role_cmd[0])
    self.assertIn(_DUMMY_DATABRICKS_USER, role_cmd[0])
    self.assertIn(
        f'CREATE DATABASE postgres OWNER "{_DUMMY_DB_USERNAME}"', db_cmd[0]
    )


if __name__ == '__main__':
  unittest.main()
