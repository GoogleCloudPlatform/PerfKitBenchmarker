# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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
"""Classes for Google Cloud Key Management Service (KMS) Keys.

For documentation see https://cloud.google.com/kms/docs.

Assumes that an existing keyring has already been created. The keyring is not
added as a PKB resource because they cannot be deleted.
"""

import dataclasses
import logging
from typing import Any, Dict, Optional

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import key
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.gcp import util


flags.DEFINE_string(
    'cloud_kms_keyring_name',
    None,
    (
        'The name of the existing keyring to create the key in. '
        'Uses "test" if unset.'
    ),
)

FLAGS = flags.FLAGS

_DEFAULT_KEYRING = 'test'  # Existing keyring
_DEFAULT_LOCATION = 'global'
_DEFAULT_ALGORITHM = 'google-symmetric-encryption'
_DEFAULT_PURPOSE = 'encryption'
_DEFAULT_PROTECTION_LEVEL = 'software'

_VALID_PURPOSES = [
    'asymmetric-encryption',
    'asymmetric-signing',
    'encryption',
    'mac',
]
_VALID_PROTECTION_LEVELS = ['software', 'hsm', 'external', 'external-vpc']

_TEST_FILE = 'hello_world.txt'


@dataclasses.dataclass
class GcpKeySpec(key.BaseKeySpec):
  """Configurable options of a KMS Key.

  Attributes:
    purpose: Purpose of the key in terms of encryption. See
      https://cloud.google.com/sdk/gcloud/reference/kms/keys/create#--purpose.
    algorithm: Algorithm to use for the key. See
      https://cloud.google.com/kms/docs/algorithms.
    protection_level: Storage environment, see
      https://cloud.google.com/kms/docs/resource-hierarchy#protection_level.
    location: Regions, multi-regions, global, etc. Run `gcloud kms locations
      list`.
    keyring_name: The name of the existing keyring to add this key to.
  """

  CLOUD = 'GCP'

  purpose: str
  algorithm: str
  protection_level: str
  location: str
  keyring_name: str

  def __init__(
      self,
      component_full_name: str,
      flag_values: Optional[flags.FlagValues] = None,
      **kwargs,
  ):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls) -> dict[str, Any]:
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments
      to construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'purpose': (
            option_decoders.EnumDecoder,
            {'valid_values': _VALID_PURPOSES, 'default': _DEFAULT_PURPOSE},
        ),
        'algorithm': (
            option_decoders.StringDecoder,
            {'default': _DEFAULT_ALGORITHM, 'none_ok': True},
        ),
        'protection_level': (
            option_decoders.EnumDecoder,
            {
                'valid_values': _VALID_PROTECTION_LEVELS,
                'default': _DEFAULT_PROTECTION_LEVEL,
            },
        ),
        'location': (
            option_decoders.StringDecoder,
            {'default': _DEFAULT_LOCATION, 'none_ok': True},
        ),
        'keyring_name': (
            option_decoders.StringDecoder,
            {'default': _DEFAULT_KEYRING, 'none_ok': True},
        ),
    })
    return result

  @classmethod
  def _ApplyFlags(
      cls, config_values: dict[str, Any], flag_values: flags.FlagValues
  ) -> None:
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. May be
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.
    """
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['cloud_kms_keyring_name'].present:
      config_values['keyring_name'] = flag_values.cloud_kms_keyring_name


class GcpKey(key.BaseKey):
  """Object representing a GCP Cloud Key Management Service Key."""

  CLOUD = 'GCP'

  def __init__(self, spec: GcpKeySpec):
    super().__init__()
    self.name = f'pkb-{FLAGS.run_uri}'
    self.purpose = spec.purpose
    self.algorithm = spec.algorithm
    self.protection_level = spec.protection_level
    self.location = spec.location
    self.keyring_name = spec.keyring_name
    self.project = FLAGS.project

  def _Create(self) -> None:
    """Creates the key in an existing keyring."""
    cmd = util.GcloudCommand(self, 'kms', 'keys', 'create', self.name)
    cmd.flags['keyring'] = self.keyring_name
    cmd.flags['location'] = self.location
    cmd.flags['purpose'] = self.purpose
    cmd.flags['protection-level'] = self.protection_level
    cmd.flags['default-algorithm'] = self.algorithm
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()
    cmd.Issue(raise_on_failure=True)

  def _Encrypt(self, input_file: str, output_file: str):
    cmd = util.GcloudCommand(self, 'kms', 'encrypt')
    cmd.flags['keyring'] = self.keyring_name
    cmd.flags['location'] = self.location
    cmd.flags['key'] = self.name
    cmd.flags['plaintext-file'] = input_file
    cmd.flags['ciphertext-file'] = output_file
    cmd.Issue(raise_on_failure=True)

  def _EncryptSimple(self):
    input_file = data.ResourcePath(f'key/{_TEST_FILE}')
    output_file = vm_util.PrependTempDir(f'{_TEST_FILE}.encrypted')
    self._Encrypt(input_file, output_file)

  def _GetPublicKey(self):
    cmd = util.GcloudCommand(
        self, 'kms', 'keys', 'versions', 'get-public-key', '1'
    )
    cmd.flags['keyring'] = self.keyring_name
    cmd.flags['location'] = self.location
    cmd.flags['key'] = self.name
    cmd.flags['output-file'] = vm_util.PrependTempDir(f'{self.name}-public-key')
    cmd.Issue(raise_on_failure=True)

  def _IsReady(self) -> bool:
    try:
      # TODO(user): create separate subclasses based on purpose.
      if self.purpose == 'encryption':
        self._EncryptSimple()
      elif self.purpose == 'asymmetric-encryption':
        self._GetPublicKey()
    except errors.Error:
      return False
    return True

  def _Delete(self) -> None:
    """Deletes the key."""
    # Keys are deleted by default automatically after 24 hours:
    # https://cloud.google.com/kms/docs/create-key#soft_delete
    # Raise an error which causes the delete to end on the second invocation
    # since it checks for `self.deleted``.
    self.deleted = True
    raise errors.Resource.RetryableDeletionError(
        'Keys automatically delete after a set time period.'
    )

  def _Exists(self) -> bool:
    """Returns true if the key exists."""
    cmd = util.GcloudCommand(self, 'kms', 'keys', 'describe', self.name)
    cmd.flags['keyring'] = self.keyring_name
    cmd.flags['location'] = self.location
    # Do not log error or warning when checking existence.
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not find GCP KMS Key %s.', self.name)
      return False
    return True

  def GetResourceMetadata(self) -> Dict[Any, Any]:
    """Returns useful metadata about the key."""
    return {
        'gcp_kms_key_name': self.name,
        'gcp_kms_key_purpose': self.purpose,
        'gcp_kms_key_protection_level': self.protection_level,
        'gcp_kms_key_algorithm': self.algorithm,
        'gcp_kms_key_location': self.location,
        'gcp_kms_key_keyring_name': self.keyring_name,
    }
