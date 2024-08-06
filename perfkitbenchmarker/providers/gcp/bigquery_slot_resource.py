"""Resource for provisioning and lifecycle management of flat-rate Bigquery slots.

This resource represents a single Bigquery flat rate capacity commitment (see
https://cloud.google.com/bigquery/docs/reservations-intro#commitments). In
order to use these slots to run queries, a reservation and reservation
assignment must be made separately in order to correctly associate these slots
with a project.
Currently Flex slots are the only payment plan that may be provisioned.
"""

import datetime
import json
import logging
import time

from absl import flags
from perfkitbenchmarker import edw_compute_resource
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util


FLAGS = flags.FLAGS


class BigquerySlotResource(edw_compute_resource.EdwComputeResource):
  """Class representing a flat-rate slot allocation on a BigQuery project."""

  CLOUD = provider_info.GCP
  SERVICE_TYPE = 'bigquery_slots'

  def __init__(self, edw_service_spec):
    super().__init__(edw_service_spec)
    self.region = FLAGS.bq_slot_allocation_region
    self.project = FLAGS.bq_slot_allocation_project
    self.slot_num = FLAGS.bq_slot_allocation_num

  def _Create(self):
    """Provision flat-rate slots on a specific pre-existing Bigquery project.

    Creates a capacity commitment for flat-rate slots in a Bigquery project.
    In order to use these slots for computation, a reservation and reservation
    assignment must be created and associated with the appropriate project.
    Currently, only flex slots may be provisioned in this way.
    """
    logging.info('Allocating slots')
    self.marked_active = False
    cmd = [
        'bq',
        'mk',
        '--capacity_commitment=true',
        f'--project_id={self.project}',
        f'--location={self.region}',
        '--plan=FLEX',
        '--format=prettyjson',
        f'--slots={self.slot_num}',
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    logging.info('Flex Slot Allocation Result: %s', str(stdout))
    commitment_result = json.loads(stdout)
    self.compute_resource_identifier = str.split(
        commitment_result['name'], '/'
    )[-1]
    if commitment_result['state'] == 'ACTIVE':
      self.marked_active = True
    self.final_commitment_start_time = datetime.datetime.now()

  def _IsReady(self):
    """Checks that capacity commitment state is ACTIVE."""
    if self.marked_active:
      return True
    cmd = [
        'bq',
        'show',
        '--format=prettyjson',
        '--capacity_commitment',
        f'{self.project}:{self.region}.{self.compute_resource_identifier}',
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    state = json.loads(stdout)['state']
    if state == 'ACTIVE':
      self.marked_active = True
      return True
    return False

  def _Exists(self) -> bool:
    """Method to validate the existence of a Bigquery cluster.

    Returns:
      Boolean value indicating the existence of a cluster.
    """
    if self.compute_resource_identifier is None:
      return False
    cmd = [
        'bq',
        'ls',
        '--format=prettyjson',
        '--capacity_commitment',
        f'--project_id={self.project}',
        f'--location={self.region}',
    ]
    try:
      stdout, _, _ = vm_util.IssueCommand(cmd)
      logging.info('bq_exists_response: %s', stdout)
      if stdout.strip() == 'No capacity commitments found.':
        return False
      commitments_list = json.loads(stdout)
      for commitment in commitments_list:
        if (
            str.split(commitment['name'], '/')[-1]
            == self.compute_resource_identifier
        ):
          return True
      return False
    except (json.JSONDecodeError, errors.VmUtil.IssueCommandError) as e:
      logging.info('bq response: %s', e)
      return False

  def _Delete(self):
    """Delete a BigQuery flat-rate slot capacity commitment.

    Removes the capacity commitment created by this resource.
    """

    wait_time = datetime.datetime.now() - self.final_commitment_start_time
    if wait_time.total_seconds() < 61:
      time.sleep(61 - wait_time.total_seconds())
    cmd = [
        'bq',
        'rm',
        f'--project_id={self.project}',
        f'--location={self.region}',
        '--format=prettyjson',
        '--capacity_commitment=true',
        '--force',
        self.compute_resource_identifier,
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    logging.info('Flex Slot Deletion Result: %s', str(stdout))
