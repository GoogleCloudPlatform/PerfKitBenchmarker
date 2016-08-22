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
"""Utilities for working with ProfitBricks resources."""

import logging
import requests

from perfkitbenchmarker import errors
from perfkitbenchmarker.providers import profitbricks
from perfkitbenchmarker.providers.profitbricks import \
    profitbricks_machine_types

# Global Values
PROFITBRICKS_API = profitbricks.PROFITBRICKS_API
FLAVORS = profitbricks_machine_types.FLAVORS


def PerformRequest(action, url, header, json=None):
    """Makes an HTTP request to the ProfitBricks REST API."""

    # Make HTTP call
    if action == 'get':
        r = requests.get(url, headers=header)
    elif action == 'post':
        r = requests.post(url, headers=header, json=json)
    elif action == 'delete':
        r = requests.delete(url, headers=header)

    # Check Response Status Code
    if r.status_code >= 300:
        action = action.upper()
        logging.info(r.text)
        raise errors.Error('%s call to %s failed, see log.' % (action,
                                                               url))

    return r


def ReturnImage(header, location):
    """Returns Ubuntu image based on zone location."""

    # Retrieve list of provider images
    url = '%s/images?depth=5' % PROFITBRICKS_API
    r = PerformRequest('get', url, header)
    response = r.json()
    logging.info('Fetching image for new VM.')

    # Search for Ubuntu image in preferred zone
    for image in response['items']:
        if('Ubuntu-14' in image['properties']['name'] and
           image['properties']['location'] == location):
            return image['id']


def ReturnFlavor(machine_type):
    """Returns RAM and Core values based on machine_type selection."""

    logging.info('Fetching flavor specs for new VM.')
    for flavor in FLAVORS:
        if(machine_type == flavor['name']):
            return flavor['ram'], flavor['cores']


def CreateDatacenter(header, location):
    """Creates a Datacenter."""

    # Build new DC body
    new_dc = {
        'properties': {
            'name': 'Perfkit DC',
            'location': location,
        },
    }

    # Make call
    logging.info('Creating Datacenter: %s in Location: %s' %
                 (new_dc['properties']['name'], location))
    url = '%s/datacenters' % PROFITBRICKS_API
    r = PerformRequest('post', url, header, json=new_dc)

    # Parse Required values from response
    status_url = r.headers['Location']
    response = r.json()
    datacenter_id = response['id']

    return datacenter_id, status_url


def CreateLan(header, datacenter):
    """Creates a LAN with public IP address."""

    # Build new LAN body
    new_lan = {
        'properties': {
            'name': 'lan1',
            'public': True,
        },
    }

    # Make call
    logging.info('Creating LAN')
    url = '%s/datacenters/%s/lans' % (PROFITBRICKS_API, datacenter)
    r = PerformRequest('post', url, header, json=new_lan)

    # Parse Required values from response
    status_url = r.headers['Location']
    response = r.json()
    lan_id = response['id']

    return lan_id, status_url
