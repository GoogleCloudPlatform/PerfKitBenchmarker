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

"""Utilities for working with Azure resources."""


import json
import re
from typing import Any, Dict, Set

from absl import flags
from perfkitbenchmarker import context
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure

AZURE_PATH = azure.AZURE_PATH
OUTPUT_JSON = ['--output', 'json']
FLAGS = flags.FLAGS


def GetAzureStorageConnectionString(
    storage_account_name: str,
    resource_group: str | None = None,
    subscription: str | None = None,
):
  """Get connection string."""
  cmd = [
      AZURE_PATH,
      'storage',
      'account',
      'show-connection-string',
      '--name',
      storage_account_name,
  ] + OUTPUT_JSON
  if resource_group:
    cmd.extend(['--resource-group', resource_group])
  if subscription:
    cmd.extend(['--subscription', subscription])
  stdout, _ = vm_util.IssueRetryableCommand(cmd)

  response = json.loads(stdout)
  return response['connectionString']


def GetAzureStorageAccountKey(storage_account_name, resource_group_args):
  """Get storage account key."""
  stdout, _ = vm_util.IssueRetryableCommand(
      [
          AZURE_PATH,
          'storage',
          'account',
          'keys',
          'list',
          '--account-name',
          storage_account_name,
      ]
      + resource_group_args
      + OUTPUT_JSON
  )

  response = json.loads(stdout)
  # A new storage account comes with two keys, but we only need one.
  assert response[0]['permissions'].lower() == 'full'
  return response[0]['value']


def FormatTag(key, value):
  """Format an individual tag for use with the --tags param of Azure CLI."""
  return '{}={}'.format(key, value)


def FormatTags(tags_dict):
  """Format a dict of tags into arguments for 'tag' parameter.

  Args:
    tags_dict: Tags to be formatted.

  Returns:
    A list of tags formatted as arguments for 'tag' parameter.
  """
  return [FormatTag(k, v) for k, v in sorted(tags_dict.items())]


def GetResourceTags(timeout_minutes):
  """Gets a dict of tags.

  Args:
    timeout_minutes: int, Timeout used for setting the timeout_utc tag.

  Returns:
    A dict contains formatted tags.
  """
  benchmark_spec = context.GetThreadBenchmarkSpec()
  return benchmark_spec.GetResourceTags(timeout_minutes)


def GetTags(timeout_minutes):
  """Gets a list of tags to be used with the --tags param of Azure CLI.

  Args:
    timeout_minutes: int, Timeout used for setting the timeout_utc tag.

  Returns:
    A string contains formatted tags.
  """
  return FormatTags(GetResourceTags(timeout_minutes))


def GetTagsJson(timeout_minutes):
  """Gets a JSON string of tags to be used with the --set param of Azure CLI.

  Args:
    timeout_minutes: int, Timeout used for setting the timeout_utc tag.

  Returns:
    A string contains json formatted tags.
  """
  return 'tags={}'.format(json.dumps(GetResourceTags(timeout_minutes)))


def _IsRegion(zone_or_region):
  """Returns whether "zone_or_region" is a region."""
  return re.match(r'[a-z]+[0-9]?$', zone_or_region, re.IGNORECASE)


def _IsRecommendedRegion(json_object: Dict[str, Any]) -> bool:
  return json_object['metadata']['regionCategory'] == 'Recommended'


def IsZone(zone_or_region):
  """Returns whether "zone_or_region" is a zone.

  Args:
    zone_or_region: string, Azure zone or region. Format for Azure availability
      zone support is "region-availability_zone". Example: eastus2-1 specifies
      Azure region eastus2 with availability zone 1.
  """

  return re.match(r'[a-z]+[0-9]?-[0-9]$', zone_or_region, re.IGNORECASE)


def GetRegionFromZone(zone_or_region: str) -> str:
  """Returns the region a zone is in (or "zone_or_region" if it's a region)."""
  if _IsRegion(zone_or_region):
    return zone_or_region
  if IsZone(zone_or_region):
    return zone_or_region[:-2]

  raise ValueError(
      '%s is not a valid Azure zone or region name' % zone_or_region
  )


def GetZonesInRegion(region: str) -> Set[str]:
  """Returns a set of zones in the region."""
  # As of 2021 all Azure AZs are numbered 1-3 for eligible regions.
  return {f'{region}-{i}' for i in range(1, 4)}


def ShouldKeepZoneFromCLI(zone: str) -> bool:
  """Filter out zones that we can't access."""
  if 'EUAP' in zone:
    return False
  return True


def GetZonesFromMachineType(machine_type: str) -> Set[str]:
  """Returns a set of zones for a machine type."""
  stdout, _ = vm_util.IssueRetryableCommand(
      [AZURE_PATH, 'vm', 'list-skus', '--size', machine_type]
  )
  zones = set()
  for item in json.loads(stdout):
    for location_info in item['locationInfo']:
      region = location_info['location']
      for zone in location_info['zones']:
        if ShouldKeepZoneFromCLI(f'{region}-{zone}'):
          zones.add(f'{region}-{zone}')
  return zones


def GetAllRegions() -> Set[str]:
  """Returns all valid regions."""
  stdout, _ = vm_util.IssueRetryableCommand(
      [AZURE_PATH, 'account', 'list-locations', '--output', 'json']
  )
  # Filter out staging regions from the output.
  return {
      item['name'] for item in json.loads(stdout) if _IsRecommendedRegion(item)
  }


def GetAllZones() -> Set[str]:
  """Returns all valid availability zones."""
  zones = set()
  for region in GetAllRegions():
    zones.update(GetZonesInRegion(region))
  return zones


def GetGeoFromRegion(region: str) -> str:
  """Gets valid geo from the region, i.e. region westus2 returns US."""
  stdout, _ = vm_util.IssueRetryableCommand([
      AZURE_PATH,
      'account',
      'list-locations',
      '--output',
      'json',
      '--query',
      f"[?name == '{region}'].metadata.geographyGroup",
  ])
  return stdout.splitlines()[1].strip('" ')


def GetRegionsInGeo(geo: str) -> Set[str]:
  """Gets valid regions in the geo."""
  stdout, _ = vm_util.IssueRetryableCommand([
      AZURE_PATH,
      'account',
      'list-locations',
      '--output',
      'json',
      '--query',
      f"[?metadata.geographyGroup == '{geo}']",
  ])
  return {
      item['name'] for item in json.loads(stdout) if _IsRecommendedRegion(item)
  }


def GetAvailabilityZoneFromZone(zone_or_region):
  """Returns the Availability Zone from a zone."""
  if IsZone(zone_or_region):
    return zone_or_region[-1]
  if _IsRegion(zone_or_region):
    return None
  raise ValueError('%s is not a valid Azure zone' % zone_or_region)


def GetMachineFamily(machine_type):
  """Returns the machine family of a machine type.

  The family is the machine type with the number of vCPUs removed, hence all
  machines from the same series shows up as having the same family.
  See testGetMachineFamily in azure_util_test.py for examples.

  Args:
    machine_type: Azure machine type
  """
  if not machine_type:
    return None
  match = re.match(r'^(.*?)\d+(.*)$', machine_type)
  if match:
    return match.group(1) + match.group(2)
  return None


def GetMachineSeriesNumber(machine_type: str) -> int:
  """Returns the series numberof a machine type.

  Not to be confused with the boot architecture generation number.
  https://learn.microsoft.com/en-us/azure/virtual-machines/generation-2

  Args:
    machine_type: Azure machine type
  """
  match = re.search(r'_v([0-9]+)$', machine_type)
  if match:
    return int(match.group(1))
  # Azure only adds a v after the first series,
  return 1
