"""Tests for messaging_service.py.

This is the common interface used by the benchmark VM to run the benchmark.
"""

import os
import unittest
from unittest import mock

from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.messaging_service import MessagingService
from tests import pkb_common_test_case

MESSAGING_SERVICE_DATA_DIR = 'messaging_service'


class MessagingServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.client = mock.create_autospec(
        virtual_machine.BaseVirtualMachine, instance=True)
    self.messaging_service = MessagingService(self.client)

  def testPrepare(self):
    self.messaging_service.prepare()

    self.client.assert_has_calls([
        mock.call.Install('python3'),
        mock.call.Install('pip3'),
        mock.call.RemoteCommand('sudo pip3 install absl-py numpy')
    ])

    datafile_path = os.path.join(MESSAGING_SERVICE_DATA_DIR,
                                 'messaging_service_client.py')
    self.client.PushDataFile.assert_called_with(datafile_path)

if __name__ == '__main__':
  unittest.main()
