"""Tests for Messaging Service Scripts end-to-end worker_tests' subprocess code."""

import os
import unittest
from unittest import mock

from perfkitbenchmarker.scripts.messaging_service_scripts.common import app
from perfkitbenchmarker.scripts.messaging_service_scripts.common import errors
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import protocol
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import publisher
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import receiver
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import worker_utils
from tests import pkb_common_test_case


class MessagingServiceScriptsE2ECommunicatorTest(
    pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.input_conn_mock = mock.Mock()
    self.output_conn_mock = mock.Mock()
    self.communicator = worker_utils.Communicator(self.input_conn_mock,
                                                  self.output_conn_mock)

  def testSend(self):
    self.communicator.send('hello')
    self.input_conn_mock.assert_not_called()
    self.output_conn_mock.send.assert_called_once_with('hello')

  def testGreet(self):
    self.communicator.greet()
    self.input_conn_mock.assert_not_called()
    self.output_conn_mock.send.assert_called_once_with(protocol.Ready())

  def testAwaitFromMainNoAckObject(self):
    self.input_conn_mock.recv.return_value = 'incoming'
    self.communicator.await_from_main(str)
    self.input_conn_mock.recv.assert_called_once_with()
    self.output_conn_mock.send.assert_not_called()

  def testAwaitFromMainWithAckObject(self):
    self.input_conn_mock.recv.return_value = 'incoming'
    self.communicator.await_from_main(str, 'alright')
    self.input_conn_mock.recv.assert_called_once_with()
    self.output_conn_mock.send.assert_called_once_with('alright')

  def testAwaitFromMainUnexpectedObjClass(self):
    self.input_conn_mock.recv.return_value = 42
    with self.assertRaises(errors.EndToEnd.ReceivedUnexpectedObjectError):
      self.communicator.await_from_main(str, 'alright')
    self.input_conn_mock.recv.assert_called_once_with()
    self.output_conn_mock.send.assert_not_called()


class BaseSubprocessTest(pkb_common_test_case.PkbCommonTestCase):

  subprocess_module = None

  @classmethod
  def Main(cls, *args, **kwargs):
    cls.subprocess_module.main(*args, **kwargs)

  def setUp(self):
    super().setUp()
    self.flags_mock = self.enter_context(
        mock.patch.object(self.subprocess_module, 'FLAGS'))
    self.communicator_mock = self.enter_context(
        mock.patch.object(worker_utils, 'Communicator'))
    self.get_client_class_mock = self.enter_context(
        mock.patch.object(app.App, 'get_client_class'))
    self.time_ns_mock = self.enter_context(mock.patch('time.time_ns'))
    self.sched_setaffinity_mock = self.enter_context(
        mock.patch.object(os, 'sched_setaffinity'))
    self.communicator_instance_mock = self.communicator_mock.return_value
    self.client_instance_mock = self.get_client_class_mock(
    ).from_flags.return_value
    self.input_conn_mock = mock.Mock()
    self.output_conn_mock = mock.Mock()
    self.parent_mock = mock.Mock()
    self.parent_mock.attach_mock(self.communicator_instance_mock,
                                 'communicator')
    self.parent_mock.attach_mock(self.client_instance_mock, 'client')
    self._curr_timens = 1632957090714891010

    def GetTimeNs():
      while True:
        yield self._curr_timens
        self._curr_timens += 1_000_000_000

    self.time_ns_mock.side_effect = GetTimeNs()

  def testInitialization(self):
    self.Main(
        input_conn=self.input_conn_mock,
        output_conn=self.output_conn_mock,
        serialized_flags='--foo=bar\n--bar=qux',
        app=app.App(),
        iterations=0,
    )
    self.sched_setaffinity_mock.assert_not_called()
    self.flags_mock.assert_called_once_with(['--foo=bar', '--bar=qux'],
                                            known_only=True)
    self.get_client_class_mock().from_flags.assert_called_once_with()
    self.communicator_mock.assert_called_once_with(self.input_conn_mock,
                                                   self.output_conn_mock)
    self.communicator_instance_mock.greet.assert_called_once_with()

  def testSchedSetAffinity(self):
    self.Main(
        input_conn=self.input_conn_mock,
        output_conn=self.output_conn_mock,
        serialized_flags='--foo=bar\n--bar=qux',
        app=app.App(),
        pinned_cpus={1, 2, 3},
        iterations=0,
    )
    self.sched_setaffinity_mock.assert_called_once_with(0, {1, 2, 3})


class MessagingServiceScriptsE2EReceiverTest(BaseSubprocessTest):

  subprocess_module = receiver

  def testMainLoop(self):
    self.Main(
        input_conn=self.input_conn_mock,
        output_conn=self.output_conn_mock,
        serialized_flags='--foo=bar\n--bar=qux',
        app=app.App(),
        iterations=1,
    )
    reception_report = self.communicator_instance_mock.send.call_args[0][0]
    pull_timestamp = reception_report.receive_timestamp
    ack_timestamp = reception_report.ack_timestamp
    self.assertEqual(ack_timestamp - pull_timestamp, 1_000_000_000)
    self.parent_mock.assert_has_calls([
        mock.call.communicator.greet(),
        mock.call.communicator.await_from_main(protocol.Consume,
                                               protocol.AckConsume()),
        mock.call.client.pull_message(),
        mock.call.client.acknowledge_received_message(mock.ANY),
        mock.call.communicator.send(
            protocol.ReceptionReport(
                receive_timestamp=pull_timestamp, ack_timestamp=ack_timestamp)),
    ])

  def testMainLoopError(self):
    error = Exception('Too bad')
    self.client_instance_mock.pull_message.side_effect = error
    self.Main(
        input_conn=self.input_conn_mock,
        output_conn=self.output_conn_mock,
        serialized_flags='--foo=bar\n--bar=qux',
        app=app.App(),
        iterations=1,
    )
    self.parent_mock.assert_has_calls([
        mock.call.communicator.greet(),
        mock.call.communicator.await_from_main(protocol.Consume,
                                               protocol.AckConsume()),
        mock.call.client.pull_message(),
        mock.call.communicator.send(
            protocol.ReceptionReport(receive_error=repr(error))),
    ])


class MessagingServiceScriptsE2EPublisherTest(BaseSubprocessTest):

  subprocess_module = publisher

  def testMainLoop(self):
    self.Main(
        input_conn=self.input_conn_mock,
        output_conn=self.output_conn_mock,
        serialized_flags='--foo=bar\n--bar=qux',
        app=app.App(),
        iterations=1,
    )
    self.parent_mock.assert_has_calls([
        mock.call.communicator.greet(),
        mock.call.communicator.await_from_main(protocol.Publish),
        mock.call.client.generate_random_message(self.flags_mock.message_size),
        mock.call.client.publish_message(mock.ANY),
        mock.call.communicator.send(
            protocol.AckPublish(publish_timestamp=self._curr_timens)),
    ])

  def testMainLoopError(self):
    error = Exception('Too bad')
    self.client_instance_mock.publish_message.side_effect = error
    self.Main(
        input_conn=self.input_conn_mock,
        output_conn=self.output_conn_mock,
        serialized_flags='--foo=bar\n--bar=qux',
        app=app.App(),
        iterations=1,
    )
    self.parent_mock.assert_has_calls([
        mock.call.communicator.greet(),
        mock.call.communicator.await_from_main(protocol.Publish),
        mock.call.client.generate_random_message(self.flags_mock.message_size),
        mock.call.client.publish_message(mock.ANY),
        mock.call.communicator.send(
            protocol.AckPublish(publish_error=repr(error))),
    ])


# Don't run the BaseSubprocessTest itself
del BaseSubprocessTest

if __name__ == '__main__':
  unittest.main()
