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
"""Utilities for working with Oracle Cloud Infrastructure (OCI) resources."""

import json
import re
from typing import Any

from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.oci import flags as oci_flags

OCI_PREFIX = ['oci']

# Lifecycle states after which a resource no longer exists.
DELETED_STATES = frozenset(['TERMINATING', 'TERMINATED'])

# Zones may carry an availability domain suffix, e.g. us-ashburn-1-ad-2.
_ZONE_WITH_AD_RE = re.compile(r'^(?P<region>.+)-ad-(?P<ad>\d+)$')


def GetRegionFromZone(zone: str) -> str:
  """Returns the region of a zone like 'us-ashburn-1' or 'us-ashburn-1-ad-2'."""
  match = _ZONE_WITH_AD_RE.match(zone)
  return match.group('region') if match else zone


def GetAvailabilityDomainIndexFromZone(zone: str) -> int | None:
  """Returns the 1-based AD number from a zone's "-ad-N" suffix, if any."""
  match = _ZONE_WITH_AD_RE.match(zone)
  return int(match.group('ad')) if match else None


def GetCompartmentArgs() -> list[str]:
  """Returns the --compartment-id argument if --oci_compartment_id is set."""
  if oci_flags.OCI_COMPARTMENT_ID.value:
    return ['--compartment-id', oci_flags.OCI_COMPARTMENT_ID.value]
  return []


def OciCommand(args: list[str], region: str | None = None) -> list[str]:
  """Returns a full oci command line for the given arguments."""
  cmd = OCI_PREFIX + [str(arg) for arg in args]
  if region:
    cmd += ['--region', region]
  cmd += ['--profile', oci_flags.OCI_PROFILE.value]
  return cmd


def IssueOciCommand(
    args: list[str], region: str | None = None, **kwargs
) -> tuple[Any, str, int]:
  """Runs an oci command and parses its JSON output.

  Args:
    args: Arguments to the oci command, e.g. ['network', 'vcn', 'get'].
    region: The OCI region to run the command against.
    **kwargs: Passed through to vm_util.IssueCommand.

  Returns:
    A tuple of the parsed JSON output (None if there was no output), stderr
    and the return code.
  """
  stdout, stderr, retcode = vm_util.IssueCommand(
      OciCommand(args, region), **kwargs
  )
  response = None
  if stdout and stdout.strip():
    try:
      response = json.loads(stdout)
    except ValueError:
      if not retcode:
        raise
  return response, stderr, retcode


def GetLifecycleState(args: list[str], region: str) -> str | None:
  """Returns the lifecycle state of a resource, or None if it is not found.

  Args:
    args: A 'get' command for the resource, e.g. ['network', 'vcn', 'get',
      '--vcn-id', vcn_id].
    region: The OCI region of the resource.
  """
  response, _, retcode = IssueOciCommand(args, region, raise_on_failure=False)
  if retcode or not response:
    return None
  return response['data']['lifecycle-state']


def ResourceExists(args: list[str], region: str) -> bool:
  """Returns whether the resource described by a 'get' command exists."""
  state = GetLifecycleState(args, region)
  return state is not None and state not in DELETED_STATES


def GetAvailabilityDomains(region: str) -> list[str]:
  """Returns the names of the availability domains in a region."""
  response, _, _ = IssueOciCommand(
      ['iam', 'availability-domain', 'list'] + GetCompartmentArgs(), region
  )
  return [ad['name'] for ad in response['data']]


def GetAvailabilityDomain(zone: str) -> str:
  """Returns the availability domain to use for a zone.

  Args:
    zone: A region such as 'us-ashburn-1', optionally with an availability
      domain suffix such as 'us-ashburn-1-ad-2'.

  Raises:
    errors.Config.InvalidValue: If the zone names an AD that does not exist.
  """
  if oci_flags.OCI_AVAILABILITY_DOMAIN.value:
    return oci_flags.OCI_AVAILABILITY_DOMAIN.value
  availability_domains = GetAvailabilityDomains(GetRegionFromZone(zone))
  index = GetAvailabilityDomainIndexFromZone(zone)
  if index is None:
    return availability_domains[0]
  for availability_domain in availability_domains:
    if availability_domain.upper().endswith(f'-AD-{index}'):
      return availability_domain
  raise errors.Config.InvalidValue(
      f'Zone {zone} refers to availability domain {index}, but the region '
      f'only has: {", ".join(availability_domains)}.'
  )


def GetImageId(
    operating_system: str,
    operating_system_version: str,
    shape: str,
    region: str,
) -> str:
  """Returns the newest platform image OCID for an OS that supports a shape."""
  response, _, _ = IssueOciCommand(
      [
          'compute',
          'image',
          'list',
          '--all',
          '--operating-system',
          operating_system,
          '--operating-system-version',
          operating_system_version,
          '--shape',
          shape,
          '--sort-by',
          'TIMECREATED',
          '--sort-order',
          'DESC',
      ]
      + GetCompartmentArgs(),
      region,
  )
  images = response['data'] if response else []
  if not images:
    raise errors.Config.InvalidValue(
        f'No {operating_system} {operating_system_version} image found for '
        f'shape {shape} in {region}.'
    )
  return images[0]['id']


def GetImageIdFromName(name: str, shape: str, region: str) -> str:
  """Returns the OCID of the image with the given display name."""
  response, _, _ = IssueOciCommand(
      [
          'compute',
          'image',
          'list',
          '--all',
          '--display-name',
          name,
          '--shape',
          shape,
      ]
      + GetCompartmentArgs(),
      region,
  )
  images = response['data'] if response else []
  if not images:
    raise errors.Config.InvalidValue(
        f'No image named {name} found for shape {shape} in {region}.'
    )
  return images[0]['id']


def GetDefaultTags(timeout_minutes: int | None = None) -> dict[str, str]:
  """Returns the default tags for an OCI resource created by PKB.

  Args:
    timeout_minutes: Timeout used for setting the timeout_utc tag.
  """
  benchmark_spec = context.GetThreadBenchmarkSpec()
  if not benchmark_spec:
    return {}
  return benchmark_spec.GetResourceTags(timeout_minutes)


def MakeFormattedDefaultTags(timeout_minutes: int | None = None) -> str:
  """Returns the default tags formatted for the --freeform-tags argument."""
  return json.dumps(GetDefaultTags(timeout_minutes), sort_keys=True)


def MakeDnsLabel(prefix: str, name: str) -> str:
  """Returns a valid DNS label (alphanumeric, max 15 chars) for a resource."""
  return (prefix + re.sub(r'[^a-zA-Z0-9]', '', name))[:15].lower()
