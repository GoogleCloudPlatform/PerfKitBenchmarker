#!/usr/bin/env python

# Copyright 2022 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Use the Compute Engine API to watch for maintenance notices.

# Script downloaded from official samples At:
# https://github.com/GoogleCloudPlatform/python-docs-samples
# under this path compute/metadata/main.py
For more information, see the README.md under /compute.
"""

import datetime
import time

import requests


METADATA_URL = 'http://metadata.google.internal/computeMetadata/v1/'
METADATA_HEADERS = {'Metadata-Flavor': 'Google'}


def wait_for_maintenance(callback):
  """Pull events from GCE meta-data server."""
  url = METADATA_URL + 'instance/maintenance-event'
  last_maintenance_event = None
  # [START hanging_get]
  last_etag = '0'
  should_break = False
  while True:
    r = requests.get(
        url,
        params={
            'last_etag': last_etag,
            'wait_for_change': True
        },
        headers=METADATA_HEADERS)
    # During maintenance the service can return a 503 or 104 (b/259443649),
    # so these should be retried.
    if r.status_code == 503 or r.status_code == 104:
      time.sleep(1)
      continue
    r.raise_for_status()
    last_etag = r.headers['etag']
    # [END hanging_get]
    if r.text == 'NONE':
      maintenance_event = None
    else:
      maintenance_event = r.text
    if maintenance_event != last_maintenance_event:
      last_maintenance_event = maintenance_event
      should_break = callback(maintenance_event)
    if should_break:
      # GCE VM notified around 1 min before event is finalized.
      time.sleep(120)
      break


def maintenance_callback(event):
  """Callback function when an event is received."""
  now = datetime.datetime.now().timestamp()
  if event:
    print('Host_maintenance_start _at_ {}'.format(now))
  else:
    print('Host_maintenance_end _at_ {}'.format(now))
  return event is None


def main():
  wait_for_maintenance(maintenance_callback)


if __name__ == '__main__':
  main()
