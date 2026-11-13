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
from ros_tcp_endpoint.outgoing_queue import OutgoingFrame, OutgoingQueue
from ros_tcp_endpoint.service import RosService
from ros_tcp_endpoint.tcp_sender import UnityTcpSender
from ros_tcp_endpoint.unity_service import UnityService


class TestUnityServiceRoundTrip(TestCase):
    """Test the endpoint transport independently of a running ROS graph or Unity."""

    def setUp(self):
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.server = mock.Mock()
        self.sender = UnityTcpSender(self.server, service_timeout_seconds=2.0)
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

    def connect(self, receive_timeout=None, accepted=True):
        """Start the endpoint reader and return the Unity end of its socket."""
        endpoint, peer = socket.socketpair()
        endpoint.settimeout(receive_timeout)
        peer.settimeout(1.0)
        client = ClientThread(endpoint, self.server, "test", 0)

        if not accepted:
            self.addCleanup(peer.close)
            self.assertFalse(client.start_client())
            self.assertEqual(peer.recv(1), b"")
            return
        self.assertTrue(client.start_client())

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

    def test_overlapping_connection_does_not_interrupt_capture(self):
        """The second socket is closed before it can disturb a pending service."""
        unity, _ = self.connect()
        call, srv_id = self.request(unity)
        self.send_reply(unity, srv_id, include_payload=False)
        self.connect(accepted=False)
        self.assertFalse(call.done())
        unity.conn.sendall(ClientThread.serialize_message(self.service.topic, b"image"))
        self.assertIs(call.result(timeout=1.0), self.response)

    def test_silent_connection_times_out_and_releases_slot(self):
        """Even without EOF, an idle or stalled socket releases the endpoint."""
        unity, client = self.connect(receive_timeout=0.2)
        call, _ = self.request(unity)
        self.assertIn("disconnected", call.result(timeout=1.0).message)
        client.join(timeout=1.0)
        self.assertFalse(client.is_alive())

        unity, _ = self.connect()
        call, srv_id = self.request(unity)
        self.send_reply(unity, srv_id)
        self.assertIs(call.result(timeout=1.0), self.response)

    def test_write_failure_wakes_reader_and_releases_slot(self):
        """A writer failure closes the connection even while its reader is blocked."""
        unity, client = self.connect()
        unity.conn.shutdown(socket.SHUT_RD)
        call = self.calls.submit(
            UnityService.send, self.service, b"capture", SimpleNamespace(success=False, message="")
        )
        self.assertIn("disconnected", call.result(timeout=1.0).message)
        client.join(timeout=1.0)
        self.assertFalse(client.is_alive())
        self.connect()

    def test_old_ros_response_cannot_reach_replacement_socket(self):
        """A response in flight from the first session cannot cross into the next."""
        old_unity, old_client = self.connect()
        serialize = ClientThread.serialize_message
        serializing = Event()
        release = Event()

        def delayed_serialize(topic, message):
            if message == b"old":
                serializing.set()
                if not release.wait(timeout=1.0):
                    raise AssertionError("Old response did not reach the reconnect")
            return serialize(topic, message)

        self.addCleanup(release.set)
        with ThreadPoolExecutor() as workers, mock.patch.object(
            ClientThread, "serialize_message", side_effect=delayed_serialize
        ):
            old_reply = workers.submit(
                self.sender.send_ros_service_response, old_client.conn, 1, "/ros/service", b"old"
            )
            try:
                self.assertTrue(serializing.wait(timeout=1.0))
                old_unity.conn.close()
                old_client.join(timeout=1.0)
                self.assertFalse(old_client.is_alive())
                unity, client = self.connect()
            finally:
                release.set()
            old_reply.result(timeout=1.0)
            self.sender.send_ros_service_response(client.conn, 1, "/ros/service", b"new")
            command, header = unity.read_message(unity.conn)
            self.assertEqual(command, "__response")
            self.assertEqual(json.loads(header)["srv_id"], 1)
            self.assertEqual(unity.read_message(unity.conn), ("/ros/service", b"new"))

    def test_unavailable_connection_returns_a_ros_response(self):
        """Calling while disconnected returns the service's failure response."""
        response = UnityService.send(
            self.service, b"capture", SimpleNamespace(success=False, message="")
        )
        self.assertFalse(response.success)
        self.assertIn("disconnected", response.message)

    def test_timed_out_waiting_request_is_removed_before_send(self):
        """An expired ROS request cannot execute when the writer drains later."""
        endpoint, unity = socket.socketpair()
        self.addCleanup(endpoint.close)
        self.addCleanup(unity.close)
        queue = self.sender.claim_connection(endpoint)
        queue.get()  # Handshake
        self.sender.service_timeout_seconds = 0.05
        response = UnityService.send(
            self.service, b"capture", SimpleNamespace(success=False, message="")
        )
        self.assertIn("did not respond", response.message)
        self.sender.send_unity_message("/command", b"still ordered")
        frame = queue.get()
        self.assertEqual(frame.data, ClientThread.serialize_message("/command", b"still ordered"))
        self.assertEqual(queue.queued_bytes, 0)
        self.assertFalse(self.sender.services_waiting)

    def test_full_reliable_queue_rejects_request_then_disconnects_on_command(self):
        """Service admission fails explicitly, and command overflow closes the session."""
        endpoint, unity = socket.socketpair()
        self.addCleanup(endpoint.close)
        self.addCleanup(unity.close)
        unity.settimeout(1.0)
        self.sender.max_queued_messages = 1
        queue = self.sender.claim_connection(endpoint)
        queue.get()
        self.sender.send_unity_message("/command", b"first")
        response = UnityService.send(
            self.service, b"capture", SimpleNamespace(success=False, message="")
        )
        self.assertIn("outgoing queue is full", response.message)
        self.assertFalse(self.sender.services_waiting)

        self.sender.send_unity_message("/command", b"second")
        self.assertEqual(unity.recv(1), b"")
        self.assertIsNone(queue.get())
        self.assertEqual(queue.queued_bytes, 0)


class TestOutgoingQueue(TestCase):
    """Verify the traffic policy independent of the socket and ROS."""

    def test_coalescing_keeps_command_order_and_newest_state(self):
        """Replacing pending state leaves intervening commands ahead of it."""
        queue = OutgoingQueue(max_items=3, max_bytes=32)
        queue.put(OutgoingFrame(b"old", "/joints"))
        queue.put(OutgoingFrame(b"cutter"))
        queue.put(OutgoingFrame(b"latest", "/joints"))
        queue.put(OutgoingFrame(b"gripper"))
        self.assertEqual(len(queue.items), 3)
        self.assertEqual(
            [queue.get().data for _ in range(3)], [b"cutter", b"latest", b"gripper"]
        )

    def test_byte_and_item_limits_evict_only_replaceable_frames(self):
        """Either limit can evict state while keeping reliable frames in order."""
        queue = OutgoingQueue(max_items=3, max_bytes=10)
        queue.put(OutgoingFrame(b"ooo", "/joints"))
        queue.put(OutgoingFrame(b"ooo", "/other_state"))
        queue.put(OutgoingFrame(b"first"))
        queue.put(OutgoingFrame(b"new", "/joints"))
        self.assertTrue(queue.put(OutgoingFrame(b"later")))
        self.assertEqual(queue.queued_bytes, 10)
        self.assertFalse(queue.put(OutgoingFrame(b"!")))
        self.assertFalse(queue.put(OutgoingFrame(b"x" * 11, "/joints")))
        self.assertEqual(len(queue.items), 2)
        self.assertEqual([queue.get().data for _ in range(2)], [b"first", b"later"])

    def test_message_limit_drops_waiting_state_under_pressure(self):
        """A small frame count can fill the queue well before its byte limit."""
        queue = OutgoingQueue(max_items=2, max_bytes=1024)
        queue.put(OutgoingFrame(b"state", "/joints"))
        queue.put(OutgoingFrame(b"first"))
        self.assertTrue(queue.put(OutgoingFrame(b"second")))
        self.assertFalse(queue.put(OutgoingFrame(b"third")))
        self.assertEqual(len(queue.items), 2)
        self.assertEqual([queue.get().data for _ in range(2)], [b"first", b"second"])


class TestRosServiceRequests(TestCase):
    """Exercise the ROS client futures associated with Unity requests."""

    def test_concurrent_calls_keep_their_own_ros_responses(self):
        """Reply in reverse order and deliver the result to each requesting worker."""
        ros_client = mock.Mock()
        futures = {b"first": Future(), b"second": Future()}
        started = {data: Event() for data in futures}

        def call_async(data):
            started[data].set()
            return futures[data]

        ros_client.call_async.side_effect = call_async
        service = SimpleNamespace(
            cli=ros_client,
            service_class=SimpleNamespace(Request=object),
        )
        disconnected = Event()
        with (
            mock.patch("ros_tcp_endpoint.service.rclpy.ok", return_value=True),
            mock.patch(
                "ros_tcp_endpoint.service.deserialize_message", side_effect=lambda data, _: data
            ),
            ThreadPoolExecutor(max_workers=2) as workers,
        ):
            first = workers.submit(RosService.send, service, b"first", disconnected)
            second = workers.submit(RosService.send, service, b"second", disconnected)
            try:
                self.assertTrue(started[b"first"].wait(1.0))
                self.assertTrue(started[b"second"].wait(1.0))
                futures[b"second"].set_result(b"second response")
                self.assertEqual(second.result(timeout=1.0), b"second response")
                self.assertFalse(first.done())
                futures[b"first"].set_result(b"first response")
                self.assertEqual(first.result(timeout=1.0), b"first response")
            finally:
                disconnected.set()
