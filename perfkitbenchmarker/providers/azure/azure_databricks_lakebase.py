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
"""Azure Databricks Lakebase relational database provisioning and teardown.

Manages Lakebase projects through the Lakebase Postgres REST API
(/api/2.0/postgres) without requiring a specific Databricks CLI binary version
on the runner.
"""

import configparser
import json
import logging
import os
import shlex
import time
from typing import Any

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import relational_db_spec
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.providers.azure import util
import requests

FLAGS = flags.FLAGS

DEFAULT_LAKEBASE_ENGINE_VERSION = '17'
DEFAULT_LAKEBASE_PORT = 5432
DEFAULT_LAKEBASE_CU = 2.0
# Each Compute Unit (CU) has approximately 2 GiB of RAM. See
# https://learn.microsoft.com/en-us/azure/databricks/oltp/projects/manage-computes.
MEMORY_MIB_PER_CU = 2048
MIN_AUTOSCALING_CU = 0.5
MAX_AUTOSCALING_CU = 64.0
MAX_AUTOSCALING_CU_SPREAD = 16.0

DEFAULT_BRANCH_ID = 'production'
# Databricks workspace host and token are read from the standard Databricks CLI
# config file in the user's home directory.
DATABRICKS_CONF_FILE = '.databrickscfg'

HTTP_TIMEOUT_SECONDS = 60
OPERATION_POLL_INTERVAL_SECONDS = 5
OPERATION_TIMEOUT_SECONDS = 600

LAKEBASE_PROJECT_ID = flags.DEFINE_string(
    'lakebase_project_id',
    None,
    'Custom or existing Azure Databricks Lakebase project ID. Defaults to '
    'pkb-<run_uri>.',
)
LAKEBASE_ENDPOINT_ID = flags.DEFINE_string(
    'lakebase_endpoint_id',
    None,
    'Optional Lakebase compute endpoint ID.',
)
LAKEBASE_MIN_CU = flags.DEFINE_float(
    'lakebase_min_cu',
    None,
    'Minimum compute capacity in Lakebase Compute Units (CU). One CU has ~2 '
    'GiB of RAM. If only one of lakebase_min_cu or lakebase_max_cu is '
    'specified, both are set to the same value.',
)
LAKEBASE_MAX_CU = flags.DEFINE_float(
    'lakebase_max_cu',
    None,
    'Maximum compute capacity in Lakebase Compute Units (CU). One CU has ~2 '
    'GiB of RAM. Must satisfy max_cu - min_cu <= 16.',
)

_NONE_OK = {'default': None, 'none_ok': True}
# JSON fields whose values must never be logged.
_SECRET_KEYS = frozenset({'password', 'token'})


def _FormatJson(value: Any) -> str:
  """Pretty-prints a JSON-serializable value for logging, masking secrets."""

  def _Redact(v: Any) -> Any:
    if isinstance(v, dict):
      return {
          k: '<redacted>' if k in _SECRET_KEYS else _Redact(x)
          for k, x in v.items()
      }
    if isinstance(v, list):
      return [_Redact(x) for x in v]
    return v

  return json.dumps(_Redact(value), indent=2)


class AzureDatabricksLakebaseSpec(relational_db_spec.RelationalDbSpec):
  """Configurable options of an Azure Databricks Lakebase service."""

  CLOUD = provider_info.AZURE
  ENGINE = [sql_engine_utils.LAKEBASE_POSTGRES]

  lakebase_project_id: str | None
  lakebase_endpoint_id: str | None
  lakebase_min_cu: float | None
  lakebase_max_cu: float | None

  @classmethod
  def _GetOptionDecoderConstructions(cls) -> dict[str, Any]:
    """Gets decoder classes and constructor args for each configurable option."""
    result = super()._GetOptionDecoderConstructions()
    result.update({
        # Not required: Lakebase compute is sized in CUs and storage is fully
        # managed (same as SpannerSpec).
        'db_spec': (spec.PerCloudConfigDecoder, _NONE_OK),
        'db_disk_spec': (spec.PerCloudConfigDecoder, _NONE_OK),
        'lakebase_project_id': (option_decoders.StringDecoder, _NONE_OK),
        'lakebase_endpoint_id': (option_decoders.StringDecoder, _NONE_OK),
        'lakebase_min_cu': (option_decoders.FloatDecoder, _NONE_OK),
        'lakebase_max_cu': (option_decoders.FloatDecoder, _NONE_OK),
    })
    return result

  @classmethod
  def _ApplyFlags(
      cls, config_values: dict[str, Any], flag_values: flags.FlagValues
  ) -> None:
    """Modifies config options based on runtime flag values."""
    super()._ApplyFlags(config_values, flag_values)
    for flag_name in (
        'lakebase_project_id',
        'lakebase_endpoint_id',
        'lakebase_min_cu',
        'lakebase_max_cu',
    ):
      if flag_values[flag_name].present:
        config_values[flag_name] = flag_values[flag_name].value
    # Lakebase has no VM or disk to size, so drop the Azure db_spec and
    # db_disk_spec defaults inherited from benchmark configs. Some are not even
    # decodable as an AzureVmSpec (e.g. pgbench's machine_type has no tier).
    config_values.pop('db_spec', None)
    config_values.pop('db_disk_spec', None)


class AzureDatabricksLakebase(relational_db.BaseRelationalDb):
  """Object representing an Azure Databricks Lakebase PostgreSQL service."""

  REQUIRED_ATTRS = ['CLOUD', 'IS_MANAGED', 'ENGINE']
  CLOUD = provider_info.AZURE
  IS_MANAGED = True
  ENGINE = [sql_engine_utils.LAKEBASE_POSTGRES]
  DEFAULT_PORT = DEFAULT_LAKEBASE_PORT

  def __init__(self, db_spec: AzureDatabricksLakebaseSpec):
    super().__init__(db_spec)
    self.spec: Any = db_spec
    self.project_id: str = (
        self.spec.lakebase_project_id or f'pkb-{FLAGS.run_uri}'
    )
    self.instance_id: str = self.project_id
    self.endpoint_id: str | None = self.spec.lakebase_endpoint_id
    self.min_cu, self.max_cu = self._ResolveComputeUnits()
    self.port = self.DEFAULT_PORT
    self.branch_name: str = (
        f'projects/{self.project_id}/branches/{DEFAULT_BRANCH_ID}'
    )
    self.endpoint_name: str | None = (
        f'{self.branch_name}/endpoints/{self.endpoint_id}'
        if self.endpoint_id
        else None
    )
    self.dbx_host: str | None = None
    self.dbx_token: str | None = None

  @staticmethod
  def GetDefaultEngineVersion(engine: str) -> str:
    """Returns the default PostgreSQL version for Azure Databricks Lakebase."""
    if engine != sql_engine_utils.LAKEBASE_POSTGRES:
      raise relational_db.RelationalDbEngineNotFoundError(
          f'Unsupported engine {engine} for AzureDatabricksLakebase.'
      )
    return DEFAULT_LAKEBASE_ENGINE_VERSION

  def _ResolveComputeUnits(self) -> tuple[float, float]:
    """Determines (min_cu, max_cu) from spec and validates bounds."""
    spec_min = self.spec.lakebase_min_cu
    spec_max = self.spec.lakebase_max_cu
    if spec_min is not None and spec_max is not None:
      min_cu, max_cu = float(spec_min), float(spec_max)
    elif spec_min is not None:
      min_cu = max_cu = float(spec_min)
    elif spec_max is not None:
      min_cu = max_cu = float(spec_max)
    else:
      min_cu = max_cu = DEFAULT_LAKEBASE_CU

    if not (MIN_AUTOSCALING_CU <= min_cu <= max_cu <= MAX_AUTOSCALING_CU):
      raise errors.Config.InvalidValue(
          f'Lakebase compute units must satisfy {MIN_AUTOSCALING_CU} <= '
          f'lakebase_min_cu <= lakebase_max_cu <= {MAX_AUTOSCALING_CU}; got '
          f'min_cu={min_cu}, max_cu={max_cu}.'
      )
    if (max_cu - min_cu) > MAX_AUTOSCALING_CU_SPREAD:
      raise errors.Config.InvalidValue(
          'Lakebase autoscaling requires max_cu - min_cu <= '
          f'{int(MAX_AUTOSCALING_CU_SPREAD)}; got min_cu={min_cu}, '
          f'max_cu={max_cu}.'
      )

    return min_cu, max_cu

  def _LoadDatabricksAuth(self) -> tuple[str, str]:
    """Parses and gets Databricks auth data from ~/.databrickscfg.

    Credentials are read exclusively from the Databricks CLI config file in the
    user's home directory. The lower case value of the --cloud flag is used to
    choose a profile.

    Returns:
      A tuple whose first element is the workspace host address and the second
      is the secret token.

    Raises:
      errors.Config.InvalidValue: If the config file or profile is missing, or
        the profile does not define both a host and a token.
    """
    if self.dbx_host and self.dbx_token:
      return self.dbx_host, self.dbx_token

    cfg_path = os.path.join(os.path.expanduser('~'), DATABRICKS_CONF_FILE)
    conf = configparser.ConfigParser()
    conf.read(cfg_path)
    profile = FLAGS.cloud.lower()
    if not conf.has_section(profile):
      raise errors.Config.InvalidValue(
          f'Databricks profile [{profile}] not found in {cfg_path}. Lakebase '
          'requires a Databricks workspace host and token to be configured '
          'there.'
      )
    host = conf[profile].get('host')
    token = conf[profile].get('token')
    if not host or not token:
      raise errors.Config.InvalidValue(
          f'Databricks profile [{profile}] in {cfg_path} must define both '
          '"host" and "token".'
      )

    host = host.strip().rstrip('/')
    if not host.startswith(('http://', 'https://')):
      host = f'https://{host}'

    self.dbx_host = host
    self.dbx_token = token.strip()
    return self.dbx_host, self.dbx_token

  def _Request(
      self,
      method: str,
      path: str,
      params: dict[str, Any] | None = None,
      json_body: dict[str, Any] | None = None,
      allow_404: bool = False,
  ) -> dict[str, Any] | None:
    """Sends an authenticated HTTP request to the Databricks REST API.

    The request and response are logged as pretty-printed JSON with secret
    fields masked. Headers, which carry the workspace token, are never logged.

    Args:
      method: HTTP method (e.g. 'GET', 'POST', 'PATCH', 'DELETE').
      path: REST API path (e.g. '/api/2.0/postgres/projects').
      params: Optional query parameters.
      json_body: Optional JSON request payload.
      allow_404: If True, returns None on HTTP 404 instead of raising.

    Returns:
      Parsed JSON response dict, or None if allow_404 is True and HTTP 404.

    Raises:
      errors.Resource.CreationError: If the HTTP request fails.
    """
    host, token = self._LoadDatabricksAuth()
    method = method.upper()
    if not path.startswith('/'):
      path = f'/{path}'
    url = f'{host}{path}'
    logging.info(
        'Databricks API request: %s %s%s%s',
        method,
        url,
        f'\nparams: {_FormatJson(params)}' if params else '',
        f'\nbody: {_FormatJson(json_body)}' if json_body else '',
    )
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }

    # An explicit auth callable prevents a ~/.netrc in the dev environment from
    # overriding the Authorization header. See
    # https://github.com/psf/requests/issues/3929.
    def _BearerAuth(req: Any) -> Any:
      req.headers['Authorization'] = f'Bearer {token}'
      return req

    response = requests.request(
        method=method,
        url=url,
        headers=headers,
        auth=_BearerAuth,
        params=params,
        json=json_body,
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    try:
      response_log = (
          _FormatJson(response.json()) if response.text.strip() else '<empty>'
      )
    except ValueError:  # Not JSON, e.g. an HTML error page from a proxy.
      response_log = response.text
    logging.info(
        'Databricks API response: %s %s -> HTTP %s\n%s',
        method,
        url,
        response.status_code,
        response_log,
    )
    if allow_404 and response.status_code == 404:
      return None
    if not response.ok:
      raise errors.Resource.CreationError(
          f'Databricks Lakebase API {method} {path} failed with '
          f'status {response.status_code}: {response.text}'
      )
    if not response.text or not response.text.strip():
      return {}
    return response.json()

  def _WaitForOperation(self, operation_name: str) -> dict[str, Any]:
    """Polls a Lakebase long-running operation until completion."""
    deadline = time.time() + OPERATION_TIMEOUT_SECONDS
    op_path = f'/api/2.0/postgres/{operation_name.lstrip("/")}'
    while True:
      op = self._Request('GET', op_path) or {}
      if op.get('error'):
        raise errors.Resource.CreationError(
            f'Lakebase operation {operation_name} failed: {op["error"]}'
        )
      if op.get('done', False):
        return op
      if time.time() >= deadline:
        raise errors.Resource.CreationError(
            f'Timed out waiting for Lakebase operation {operation_name}.'
        )
      time.sleep(OPERATION_POLL_INTERVAL_SECONDS)

  def _WaitIfOperation(
      self, op: dict[str, Any] | None
  ) -> dict[str, Any] | None:
    """Waits for operation if response represents an active long-running operation."""
    if op and 'operations/' in op.get('name', '') and not op.get('done', False):
      return self._WaitForOperation(op['name'])
    return op

  def _BuildEndpointSettings(self) -> dict[str, Any]:
    """Constructs the endpoint autoscaling and suspension settings."""
    return {
        'autoscaling_limit_min_cu': self.min_cu,
        'autoscaling_limit_max_cu': self.max_cu,
        'no_suspension': True,
    }

  def _BuildInitialEndpointSpec(self) -> dict[str, Any]:
    """Constructs the initial read-write endpoint spec for project creation."""
    endpoint_spec = self._BuildEndpointSettings()
    if self.spec.high_availability:
      endpoint_spec['group'] = {
          'min': 2,
          'max': 2,
          'enable_readable_secondaries': True,
      }
    return endpoint_spec

  def _DiscoverEndpoint(self) -> dict[str, Any] | None:
    """Discovers the Lakebase read-write endpoint on the default branch."""
    if self.endpoint_name:
      ep = self._Request(
          'GET',
          f'/api/2.0/postgres/{self.endpoint_name.lstrip("/")}',
          allow_404=True,
      )
      if ep:
        return ep

    endpoints_resp = self._Request(
        'GET',
        f'/api/2.0/postgres/{self.branch_name.lstrip("/")}/endpoints',
        allow_404=True,
    )
    if not endpoints_resp or not endpoints_resp.get('endpoints'):
      return None

    endpoints = endpoints_resp['endpoints']
    rw_endpoint = next(
        (
            ep
            for ep in endpoints
            if ep.get('spec', {}).get('endpoint_type')
            == 'ENDPOINT_TYPE_READ_WRITE'
            or ep.get('status', {}).get('endpoint_type')
            == 'ENDPOINT_TYPE_READ_WRITE'
        ),
        endpoints[0],
    )
    self.endpoint_name = rw_endpoint.get('name')
    return rw_endpoint

  def _Create(self) -> None:
    """Creates the Azure Databricks Lakebase project."""
    pg_version = int(
        str(self.spec.engine_version or DEFAULT_LAKEBASE_ENGINE_VERSION).split(
            '.'
        )[0]
    )
    project_spec: dict[str, Any] = {
        'display_name': self.project_id,
        'pg_version': pg_version,
        'enable_pg_native_login': True,
        'default_endpoint_settings': self._BuildEndpointSettings(),
        # Standard PKB resource tags (e.g. timeout_utc) for reaping leaks.
        'custom_tags': [
            {'key': k, 'value': v}
            for k, v in util.GetResourceTags(FLAGS.timeout_minutes).items()
        ],
    }
    initial_endpoint_spec = self._BuildInitialEndpointSpec()
    logging.info(
        'Creating Azure Databricks Lakebase project %s '
        '(pg_version=%d, min_cu=%s, max_cu=%s, high_availability=%s)',
        self.project_id,
        pg_version,
        self.min_cu,
        self.max_cu,
        bool(self.spec.high_availability),
    )
    op = self._Request(
        'POST',
        '/api/2.0/postgres/projects',
        params={'project_id': self.project_id},
        json_body={
            'spec': project_spec,
            'initial_endpoint_spec': initial_endpoint_spec,
        },
    )
    op = self._WaitIfOperation(op)

    # Lakebase projects default spec.enable_pg_native_login to false upon
    # creation; explicitly PATCH the project to enable native login.
    created_spec = (
        (op or {}).get('response', {}).get('spec')
        or (op or {}).get('spec')
        or {}
    )
    if not created_spec.get('enable_pg_native_login', False):
      patch_op = self._Request(
          'PATCH',
          f'/api/2.0/postgres/projects/{self.project_id}',
          params={'update_mask': 'spec.enable_pg_native_login'},
          json_body={
              'name': f'projects/{self.project_id}',
              'spec': {'enable_pg_native_login': True},
          },
      )
      self._WaitIfOperation(patch_op)

  def _IsReady(self) -> bool:
    """Returns True if the Lakebase endpoint is active and has a DNS host."""
    endpoint = self._DiscoverEndpoint()
    if not endpoint:
      return False
    status = endpoint.get('status', {})
    state = status.get('current_state', '')
    if state == 'FAILED':
      raise errors.Resource.CreationError(
          f'Lakebase endpoint {self.endpoint_name} entered FAILED state.'
      )
    hosts = status.get('hosts', {})
    host = hosts.get('host')
    if state in ('ACTIVE', 'IDLE') and host:
      self.endpoint = host
      self.port = self.DEFAULT_PORT
      if hosts.get('read_only_host'):
        self.replica_endpoint = hosts['read_only_host']
      return True
    return False

  def _Exists(self) -> bool:
    """Returns True if the Lakebase project exists and is not soft-deleted."""
    project = self._Request(
        'GET',
        f'/api/2.0/postgres/projects/{self.project_id}',
        allow_404=True,
    )
    # GET still returns soft-deleted projects, with delete_time set, until
    # their purge_time.
    return project is not None and not project.get('delete_time')

  def _Delete(self) -> None:
    """Deletes the Azure Databricks Lakebase project."""
    logging.info(
        'Deleting Azure Databricks Lakebase project %s', self.project_id
    )
    op = self._Request(
        'DELETE',
        f'/api/2.0/postgres/projects/{self.project_id}',
        # Hard delete; a soft-deleted project is retained until its purge_time.
        params={'purge': 'true'},
        allow_404=True,
    )
    self._WaitIfOperation(op)

  def _GetCurrentDatabricksUser(self) -> str:
    """Returns the userName or applicationId of the authenticated Databricks identity."""
    me = self._Request('GET', '/api/2.0/preview/scim/v2/Me') or {}
    user_name = me.get('userName') or me.get('applicationId')
    if not user_name:
      raise errors.Resource.CreationError(
          'Unable to determine current Databricks user from /scim/v2/Me.'
      )
    return user_name

  def _GenerateDatabaseCredential(self) -> str:
    """Returns a short-lived OAuth token used once to create the native role.

    Benchmarks connect with native Postgres password auth, but a new Lakebase
    database only has an OAuth login role (for the Databricks identity that
    created it), and the Lakebase role APIs cannot create a role with a known
    password. So the native role is created with CREATE ROLE ... PASSWORD over a
    single OAuth-authenticated connection. See
    https://learn.microsoft.com/en-us/azure/databricks/oltp/projects/postgres-roles.
    """
    if not self.endpoint_name:
      self._DiscoverEndpoint()
    if not self.endpoint_name:
      raise errors.Resource.CreationError(
          'Cannot generate Lakebase credential: endpoint not found for '
          f'project {self.project_id}.'
      )
    resp = (
        self._Request(
            'POST',
            '/api/2.0/postgres/credentials',
            json_body={'endpoint': self.endpoint_name},
        )
        or {}
    )
    token = resp.get('token')
    if not token:
      raise errors.Resource.CreationError(
          'Lakebase credential response did not contain a token.'
      )
    return token

  def _ConfigureNativeDatabaseRole(self) -> None:
    """Configures the benchmark native PostgreSQL user and default database."""
    admin_user = self._GetCurrentDatabricksUser()
    oauth_token = self._GenerateDatabaseCredential()
    target_user = self.spec.database_username
    target_password = self.spec.database_password

    client_vm = self.client_vm
    client_vm.Install('postgres_client')

    escaped_user = target_user.replace("'", "''")
    escaped_ident = target_user.replace('"', '""')
    escaped_pass = target_password.replace("'", "''")
    escaped_admin_ident = admin_user.replace('"', '""')

    setup_sql = (
        'DO $$ BEGIN '
        'IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname ='
        f" '{escaped_user}') THEN "
        f'CREATE ROLE "{escaped_ident}" WITH LOGIN PASSWORD \'{escaped_pass}\' '
        'CREATEDB CREATEROLE INHERIT; '
        'ELSE '
        f'ALTER ROLE "{escaped_ident}" WITH LOGIN PASSWORD \'{escaped_pass}\' '
        'CREATEDB CREATEROLE INHERIT; '
        'END IF; '
        'BEGIN '
        'IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname ='
        " 'databricks_superuser') THEN "
        'EXECUTE format('
        "'GRANT databricks_superuser TO %I WITH ADMIN OPTION', "
        f"'{escaped_user}'); "
        'END IF; '
        'EXCEPTION WHEN OTHERS THEN NULL; '
        'END; '
        'END $$; '
        f'GRANT "{escaped_ident}" TO "{escaped_admin_ident}";'
    )
    conn_info = shlex.quote(
        f'host={self.endpoint} port={self.port} '
        f'user={admin_user} password={oauth_token} dbname=databricks_postgres'
    )
    _, stderr, retcode = client_vm.RemoteCommandWithReturnCode(
        f'psql {conn_info} -v ON_ERROR_STOP=1 -c {shlex.quote(setup_sql)}',
        ignore_failure=True,
    )
    if retcode != 0:
      raise errors.Resource.CreationError(
          f'Failed to configure native Lakebase role {target_user}: {stderr}'
      )

    create_db_sql = shlex.quote(
        f'CREATE DATABASE postgres OWNER "{escaped_ident}";'
    )
    _, stderr, retcode = client_vm.RemoteCommandWithReturnCode(
        f'psql {conn_info} -v ON_ERROR_STOP=1 -c {create_db_sql}',
        ignore_failure=True,
    )
    if retcode != 0 and 'already exists' not in stderr.lower():
      raise errors.Resource.CreationError(
          f'Failed to create default postgres database on Lakebase: {stderr}'
      )

  def _PostCreate(self) -> None:
    """Performs post-creation role and client setup for Lakebase."""
    super()._PostCreate()
    self._ConfigureNativeDatabaseRole()

  def GetResourceMetadata(self) -> dict[str, Any]:
    """Returns metadata associated with the Azure Databricks Lakebase resource."""
    metadata = super().GetResourceMetadata()
    machine_type = (
        f'{self.min_cu:g}-{self.max_cu:g}CU'
        if self.min_cu != self.max_cu
        else f'{self.max_cu:g}CU'
    )
    metadata.update({
        'zone': (
            self.spec.zones[0]
            if getattr(self.spec, 'zones', None)
            else metadata.get('client_vm_zone')
        ),
        'endpoint_group_size': 2 if self.spec.high_availability else 1,
        'lakebase_project_id': self.project_id,
        'lakebase_min_cu': self.min_cu,
        'lakebase_max_cu': self.max_cu,
        'compute_units': self.max_cu,
        # Memory at max CU in MiB, matching the units of PKB's memory metadata.
        'memory': int(self.max_cu * MEMORY_MIB_PER_CU),
        'machine_type': machine_type,
    })
    return metadata
