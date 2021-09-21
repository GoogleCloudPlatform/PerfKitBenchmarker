"""Tests for perfkitbenchmarker.resource."""

import unittest

from absl import flags
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class NonFreezeRestoreResource(resource.BaseResource):
  """Dummy class that is missing _Freeze()/_Restore()/_UpdateTimeout()."""

  def _Create(self):
    pass

  def _Delete(self):
    pass


class CreateRaisesNonFreezeRestoreResource(NonFreezeRestoreResource):
  """Dummy class that fails to create a resource."""

  def _Create(self):
    raise errors.Resource.CreationError()


class IncompleteFreezeRestoreResource(NonFreezeRestoreResource):
  """Dummy class that is missing _UpdateTimeout()."""

  def _Freeze(self):
    pass

  def _Restore(self):
    pass


class CompleteFreezeRestoreResource(IncompleteFreezeRestoreResource):
  """Complete implementation needed for Freeze()/Restore()."""

  def _UpdateTimeout(self, timeout_minutes):
    pass


def _CreateFreezeRestoreResource():
  return CompleteFreezeRestoreResource(enable_freeze_restore=True)


class ResourceTest(pkb_common_test_case.PkbCommonTestCase):

  def testDeleteResourceBadCreate(self):
    test_resource = CreateRaisesNonFreezeRestoreResource()
    mock_delete = self.enter_context(
        mock.patch.object(test_resource, '_Delete'))

    with self.assertRaises(errors.Resource.CreationError):
      test_resource.Create()
    self.assertFalse(test_resource.created)

    test_resource.Delete()
    mock_delete.assert_not_called()


class FreezeRestoreTest(pkb_common_test_case.PkbCommonTestCase):

  def testNoFreezeImplementationRaisesFreezeError(self):
    # Freeze() called with no _Freeze() implementation.
    test_resource = NonFreezeRestoreResource()
    with self.assertRaises(errors.Resource.FreezeError):
      test_resource.Freeze()

  def testNoRestoreImplementationRaisesRestoreError(self):
    # Restore() called with no _Restore() implementation.
    test_resource = NonFreezeRestoreResource()
    with self.assertRaises(errors.Resource.RestoreError):
      test_resource.Restore()

  def testFreezeNoTimeoutRaisesNotImplementedError(self):
    # Freeze() called with no _UpdateTimeout() implementation.
    test_resource = IncompleteFreezeRestoreResource()
    with self.assertRaises(NotImplementedError):
      test_resource.Freeze()

  def testRestoreNoTimeoutRaisesNotImplementedError(self):
    # Restore() called with no _UpdateTimeout() implementation.
    test_resource = IncompleteFreezeRestoreResource()
    with self.assertRaises(NotImplementedError):
      test_resource.Restore()

  def testDeleteWithFreezeErrorFailsNoisily(self):
    # By default, FreezeError should cause Delete to fail noisily.
    test_resource = _CreateFreezeRestoreResource()
    self.enter_context(
        mock.patch.object(
            test_resource, 'Freeze', side_effect=errors.Resource.FreezeError()))

    with self.assertRaises(errors.Resource.FreezeError):
      test_resource.Delete(freeze=True)

  def testDeleteWithFreezeErrorProceedsWithDeletion(self):
    test_resource = _CreateFreezeRestoreResource()
    test_resource.delete_on_freeze_error = True
    self.enter_context(
        mock.patch.object(
            test_resource, 'Freeze', side_effect=errors.Resource.FreezeError()))

    # At the start of the test the resource is not deleted.
    self.assertFalse(test_resource.deleted)

    test_resource.Delete(freeze=True)

    self.assertTrue(test_resource.deleted)

  def testCreateWithRestoreErrorFailsNoisily(self):
    # By default, RestoreError should cause Create to fail noisily.
    test_resource = _CreateFreezeRestoreResource()
    self.enter_context(
        mock.patch.object(
            test_resource,
            'Restore',
            side_effect=errors.Resource.RestoreError()))

    with self.assertRaises(errors.Resource.RestoreError):
      test_resource.Create(restore=True)

  def testCreateWithRestoreErrorProceedsWithCreation(self):
    test_resource = _CreateFreezeRestoreResource()
    test_resource.create_on_restore_error = True
    self.enter_context(
        mock.patch.object(
            test_resource,
            'Restore',
            side_effect=errors.Resource.RestoreError()))

    # At the start of the test the resource is not deleted.
    self.assertFalse(test_resource.created)

    test_resource.Create(restore=True)

    self.assertTrue(test_resource.created)

  def testExceptionsRaisedAsFreezeError(self):
    # Ensures that generic exceptions in _Freeze raised as FreezeError.
    test_resource = _CreateFreezeRestoreResource()
    self.enter_context(
        mock.patch.object(test_resource, '_Freeze', side_effect=Exception()))
    with self.assertRaises(errors.Resource.FreezeError):
      test_resource.Freeze()

  def testDeleteWithSuccessfulFreeze(self):
    test_resource = _CreateFreezeRestoreResource()
    mock_freeze = self.enter_context(
        mock.patch.object(test_resource, '_Freeze'))
    mock_update_timeout = self.enter_context(
        mock.patch.object(test_resource, '_UpdateTimeout'))

    test_resource.Delete(freeze=True)

    mock_freeze.assert_called_once()
    mock_update_timeout.assert_called_once()
    self.assertTrue(test_resource.frozen)

  def testCreateWithSuccessfulRestore(self):
    test_resource = _CreateFreezeRestoreResource()
    mock_restore = self.enter_context(
        mock.patch.object(test_resource, '_Restore'))
    mock_create_resource = self.enter_context(
        mock.patch.object(test_resource, '_CreateResource'))

    test_resource.Create(restore=True)

    mock_restore.assert_called_once()
    mock_create_resource.assert_not_called()
    self.assertFalse(test_resource.frozen)

  def testCreateWithRestoreErrorRaisesInsteadOfCreating(self):
    test_resource = _CreateFreezeRestoreResource()
    self.enter_context(
        mock.patch.object(test_resource, '_Restore', side_effect=Exception()))
    mock_create_resource = self.enter_context(
        mock.patch.object(test_resource, '_CreateResource'))

    with self.assertRaises(errors.Resource.RestoreError):
      test_resource.Create(restore=True)

    mock_create_resource.assert_not_called()

  def testRestoreNotEnabled(self):
    test_resource = CompleteFreezeRestoreResource(enable_freeze_restore=False)
    mock_restore = self.enter_context(
        mock.patch.object(test_resource, 'Restore'))

    test_resource.Create(restore=True)

    mock_restore.assert_not_called()

  def testFreezeNotEnabled(self):
    test_resource = CompleteFreezeRestoreResource(enable_freeze_restore=False)
    mock_freeze = self.enter_context(mock.patch.object(test_resource, 'Freeze'))

    test_resource.Delete(freeze=True)

    mock_freeze.assert_not_called()


if __name__ == '__main__':
  unittest.main()
