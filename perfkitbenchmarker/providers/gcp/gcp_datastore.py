# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for GCP's datastore instance."""

import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource

FLAGS = flags.FLAGS

flags.DEFINE_string('google_datastore_keyfile',
                    None,
                    'The path to Google API P12 private key file')
flags.DEFINE_string('google_datastore_serviceAccount',
                    None,
                    'The service account email associated with'
                    'datastore private key file')
flags.DEFINE_string('google_datastore_datasetId',
                    None,
                    'The project ID that has Cloud Datastore service')


class GcpDatastoreInstance(resource.BaseResource):
  """Object representing a GCP Datastore Instance."""

  def __init__(self):
        # Before YCSB Cloud Datastore supports Application Default Credential,
    # we should always make sure valid credential flags are set.
    if not FLAGS.google_datastore_keyfile:
      raise ValueError('"google_datastore_keyfile" must be set')
    if not FLAGS.google_datastore_serviceAccount:
      raise ValueError('"google_datastore_serviceAccount" must be set')
    if not FLAGS.google_datastore_datasetId:
      raise ValueError('"google_datastore_datasetId" must be set ')

    self.keyfile = FLAGS.google_datastore_keyfile
    self.run_kwargs = {
        'googledatastore.datasetId':
            FLAGS.google_datastore_datasetId,
        'googledatastore.serviceAccountEmail':
            FLAGS.google_datastore_serviceAccount,
    }

  def _Create(self):
    pass

  def _Delete(self):
    logging.warning(
        'For now, we can only manually delete all the entries via GCP portal.')

  def DeleteDatabase(self):
    self._Delete()
