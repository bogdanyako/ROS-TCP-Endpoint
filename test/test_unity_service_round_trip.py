# Copyright 2026 Ostia contributors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise Unity service replies and disconnects over local sockets."""

import json
import socket
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import ExitStack
from threading import Event
from types import SimpleNamespace
from unittest import TestCase, mock

from ros_tcp_endpoint.client import ClientThread
from ros_tcp_endpoint.tcp_sender import UnityTcpSender
from ros_tcp_endpoint.unity_service import UnityService


class TestUnityServiceRoundTrip(TestCase):
    """Test the endpoint transport independently of a running ROS graph or Unity."""

    def setUp(self):
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.server = mock.Mock()
        self.sender = UnityTcpSender(self.server, service_timeout_seconds=2.0)
        self.sender.time_between_halt_checks = 0.05
        self.server.unity_tcp_sender = self.sender
        self.server.send_unity_service = self.sender.send_unity_service_request
        self.service = SimpleNamespace(
            tcp_server=self.server,
            topic="/camera/capture",
            service_class=SimpleNamespace(Response=object),
        )
        self.response = SimpleNamespace(success=True, message="captured")
        self.calls = stack.enter_context(ThreadPoolExecutor(max_workers=1))
        stack.enter_context(mock.patch("ros_tcp_endpoint.client.serialize_message", bytes))
        self.deserialize = stack.enter_context(
            mock.patch("ros_tcp_endpoint.tcp_sender.deserialize_message", return_value=self.response)
        )

    def connect(self):
        """Start the endpoint reader and return the Unity end of its socket."""
        endpoint, peer = socket.socketpair()
        peer.settimeout(1.0)
        client = ClientThread(endpoint, self.server, "test", 0)
        client.daemon = True
        client.start()

        def disconnect():
            peer.close()
            client.join(timeout=1.0)
            self.assertFalse(client.is_alive())

        self.addCleanup(disconnect)
        unity = ClientThread(peer, self.server, "test", 0)
        self.assertEqual(unity.read_message(peer)[0], "__handshake")
        return unity, client

    def request(self, unity):
        """Start a ROS callback and read its request on the Unity socket."""
        call = self.calls.submit(
            UnityService.send, self.service, b"capture", SimpleNamespace(success=False, message="")
        )
        command, header = unity.read_message(unity.conn)
        self.assertEqual(command, "__request")
        self.assertEqual(unity.read_message(unity.conn), (self.service.topic, b"capture"))
        return call, json.loads(header)["srv_id"]

    def send_reply(self, unity, srv_id, include_payload=True):
        """Send the connector's service header and optional payload frame."""
        frame = ClientThread.serialize_command("__response", SimpleNamespace(srv_id=srv_id))
        if include_payload:
            frame += ClientThread.serialize_message(self.service.topic, b"image")
        unity.conn.sendall(frame)

    def test_reply_before_wait_is_delivered(self):
        """Force the reply to complete before the caller starts waiting for it."""
        replied = Event()

        class EarlyReplyFuture(Future):
            def result(self, timeout=None):
                if not replied.wait(timeout=1.0):
                    raise AssertionError("Unity reply was not received before the wait")
                return super().result(timeout=timeout)

            def set_result(self, result):
                super().set_result(result)
                replied.set()

        unity, _ = self.connect()
        with mock.patch("ros_tcp_endpoint.tcp_sender.Future", EarlyReplyFuture):
            call, srv_id = self.request(unity)
            self.send_reply(unity, srv_id)
            self.assertIs(call.result(timeout=1.0), self.response)

    def test_timeout_and_late_reply_leave_connection_usable(self):
        """A timed-out ID cannot terminate the reader or satisfy a new call."""
        unity, _ = self.connect()
        self.sender.service_timeout_seconds = 0.05
        call, expired_id = self.request(unity)
        failure = call.result(timeout=1.0)
        self.assertFalse(failure.success)
        self.assertIn("did not respond", failure.message)
        self.assertFalse(self.sender.services_waiting)
        self.deserialize.assert_not_called()

        self.sender.service_timeout_seconds = 2.0
        call, srv_id = self.request(unity)
        self.send_reply(unity, expired_id)
        self.send_reply(unity, srv_id)
        self.assertIs(call.result(timeout=1.0), self.response)
        self.deserialize.assert_called_once()

    def test_disconnect_during_reply_cancels_request_and_allows_reconnect(self):
        """Losing the socket between the two reply frames releases the ROS callback."""
        unity, client = self.connect()
        call, srv_id = self.request(unity)
        self.send_reply(unity, srv_id, include_payload=False)
        unity.conn.close()
        failure = call.result(timeout=1.0)
        self.assertFalse(failure.success)
        self.assertIn("disconnected", failure.message)
        self.assertFalse(self.sender.services_waiting)
        client.join(timeout=1.0)
        self.assertFalse(client.is_alive())

        unity, _ = self.connect()
        call, srv_id = self.request(unity)
        self.send_reply(unity, srv_id)
        self.assertIs(call.result(timeout=1.0), self.response)

    def test_old_connection_cannot_complete_or_cancel_new_request(self):
        """An overlapping reconnect owns its own pending service requests."""
        old_unity, old_client = self.connect()
        old_call, _ = self.request(old_unity)
        unity, _ = self.connect()
        self.assertIn("disconnected", old_call.result(timeout=1.0).message)

        call, srv_id = self.request(unity)
        self.send_reply(old_unity, srv_id)
        old_unity.conn.close()
        old_client.join(timeout=1.0)
        self.assertFalse(old_client.is_alive())
        self.assertFalse(call.done())
        self.send_reply(unity, srv_id)
        self.assertIs(call.result(timeout=1.0), self.response)

    def test_unavailable_connection_returns_a_ros_response(self):
        """Calling while disconnected returns the service's failure response."""
        response = UnityService.send(
            self.service, b"capture", SimpleNamespace(success=False, message="")
        )
        self.assertFalse(response.success)
        self.assertIn("disconnected", response.message)
