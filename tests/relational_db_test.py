"""Tests for relational_db."""

import unittest
from absl import flags
import mock
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import relational_db_spec
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


def _CreateTestVm(mock_id):
  vm = pkb_common_test_case.TestVirtualMachine(
      pkb_common_test_case.TestVmSpec('test_component_name')
  )
  vm.id = mock_id
  return vm


# Implements some abstract functions so we can instantiate BaseRelationalDb.
class TestBaseRelationalDb(relational_db.BaseRelationalDb):

  def _Create(self):
    pass

  def _Delete(self):
    pass

  def GetDefaultEngineVersion(self, engine):
    return 'test'


class RelationalDbTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    minimal_spec = {
        'cloud': 'GCP',
        'engine': 'mysql',
        'db_spec': {'GCP': {'machine_type': 'n1-standard-1'}},
        'db_disk_spec': {'GCP': {'disk_size': 500}},
    }
    self.spec = relational_db_spec.RelationalDbSpec(
        'test_component', flag_values=FLAGS, **minimal_spec
    )
    FLAGS['run_uri'].parse('test_uri')

  def test_client_vm_query_tools(self):
    test_db = TestBaseRelationalDb(self.spec)
    test_db._endpoint = 'test_endpoint'
    vm1 = _CreateTestVm('vm1')
    vm2 = _CreateTestVm('vm2')
    mock_vms = {'default': [vm1, vm2]}
    test_db.SetVms(mock_vms)

    self.assertLen(test_db.client_vms_query_tools, 2)
    self.assertEqual(test_db.client_vm_query_tools.vm.id, 'vm1')

  def test_client_vm_query_tools_correct(self):
    test_db = TestBaseRelationalDb(self.spec)
    test_db._endpoint = 'test_endpoint'
    vm1 = mock.Mock('vm1')
    vm2 = mock.Mock('vm2')
    test_db.SetVms({'default': [vm1]})

    test_db.SetVms({'default': [vm2]})

    self.assertEqual(test_db.client_vm_query_tools.vm, vm2)


if __name__ == '__main__':
  unittest.main()
