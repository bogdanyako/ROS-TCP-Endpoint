#  Copyright 2020 Unity Technologies
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import json
import math
import socket
import threading
from concurrent.futures import CancelledError, Future, TimeoutError
from time import monotonic

from rclpy.serialization import deserialize_message

from .client import ClientThread
from .exceptions import UnityServiceCallError
from .outgoing_queue import OutgoingFrame, OutgoingQueue

DEFAULT_MAX_QUEUED_MESSAGES = 128
DEFAULT_MAX_QUEUED_BYTES = 16 * 1024 * 1024
DEFAULT_REPLACEABLE_TOPICS = ("/ros2_control/joint_states",)


class UnityTcpSender:
    """
    Sends messages to Unity.
    """

    def __init__(
        self,
        tcp_server,
        service_timeout_seconds,
        max_queued_messages=DEFAULT_MAX_QUEUED_MESSAGES,
        max_queued_bytes=DEFAULT_MAX_QUEUED_BYTES,
        replaceable_topics=DEFAULT_REPLACEABLE_TOPICS,
    ):
        if not math.isfinite(service_timeout_seconds) or service_timeout_seconds <= 0:
            raise ValueError("UNITY_SERVICE_TIMEOUT_SECONDS must be finite and positive")

        self.tcp_server = tcp_server
        self.service_timeout_seconds = service_timeout_seconds
        self.max_queued_messages = max_queued_messages
        self.max_queued_bytes = max_queued_bytes
        self.replaceable_topics = frozenset(replaceable_topics)
        handshake = SysCommand_Handshake(SysCommand_Handshake_Metadata())
        self.handshake = OutgoingFrame(ClientThread.serialize_command("__handshake", handshake))
        if max_queued_messages < 1 or max_queued_bytes < len(self.handshake.data):
            raise ValueError("The Unity outgoing queue must fit at least the handshake frame")

        # The lock protects the active connection, its queue, and pending service calls.
        self.connection = None
        self.queue = None
        self.queue_lock = threading.Lock()

        self.next_srv_id = 1001
        self.services_waiting = {}

    def send_unity_info(self, text):
        self._send_log("__log", text)

    def send_unity_warning(self, text):
        self._send_log("__warn", text)

    def send_unity_error(self, text, conn=None):
        self._send_log("__error", text, conn)

    def _send_log(self, destination, text, conn=None):
        queue = self.queue
        if queue is None:
            return
        command = SysCommand_Log()
        command.text = text
        data = ClientThread.serialize_command(destination, command)
        self._enqueue(queue, OutgoingFrame(data), destination, conn)

    def send_ros_service_response(self, conn, srv_id, destination, response):
        queue = self.queue
        if queue is None:
            return
        command = SysCommand_Service()
        command.srv_id = srv_id
        header = ClientThread.serialize_command("__response", command)
        payload = ClientThread.serialize_message(destination, response)
        self._enqueue(queue, OutgoingFrame(header + payload), destination, conn)

    def send_unity_message(self, topic, message):
        queue = self.queue
        if queue is not None:
            serialized_message = ClientThread.serialize_message(topic, message)
            replaceable = topic if topic in self.replaceable_topics else None
            self._enqueue(queue, OutgoingFrame(serialized_message, replaceable), topic)

    def _enqueue(self, queue, frame, destination, conn=None):
        with self.queue_lock:
            if queue is not self.queue or (conn is not None and conn is not self.connection):
                return
            if queue.put(frame) or frame.replaceable_topic is not None:
                return
            conn = self.connection
            self._close_queue()

        self.tcp_server.logwarn(
            f"Unity outgoing queue full for {destination!r} "
            f"({len(frame.data)} bytes, limits {self.max_queued_messages} frames and "
            f"{self.max_queued_bytes} bytes). Disconnecting Unity."
        )
        self._shutdown_socket(conn)

    def send_unity_service_request(self, topic, service_class, request):
        serialized_message = ClientThread.serialize_message(topic, request)
        response = Future()
        with self.queue_lock:
            queue = self.queue
            if queue is None:
                raise UnityServiceCallError("Unity is disconnected")
            srv_id = self.next_srv_id
            self.next_srv_id += 1
            command = SysCommand_Service()
            command.srv_id = srv_id
            serialized_header = ClientThread.serialize_command("__request", command)
            timeout = self.service_timeout_seconds
            deadline = monotonic() + timeout
            if not queue.put(OutgoingFrame(
                serialized_header + serialized_message, service_id=srv_id, deadline=deadline
            )):
                raise UnityServiceCallError("Unity outgoing queue is full")
            self.services_waiting[srv_id] = response

        try:
            data = response.result(timeout=max(0, deadline - monotonic()))
        except TimeoutError as error:
            raise UnityServiceCallError(
                f"Unity did not respond within {timeout:g} seconds"
            ) from error
        except CancelledError as error:
            raise UnityServiceCallError("Unity disconnected while the request was pending") from error
        finally:
            with self.queue_lock:
                self.services_waiting.pop(srv_id, None)
                queue.discard_request(srv_id)

        return deserialize_message(data, service_class.Response())

    def send_unity_service_response(self, conn, srv_id, data):
        with self.queue_lock:
            if conn is not self.connection:
                return
            response = self.services_waiting.pop(srv_id, None)
            if response is not None:
                response.set_result(data)

    def get_registered_topic(self, topic):
        if topic in self.tcp_server.publishers_table:
            return self.tcp_server.publishers_table[topic]
        elif topic in self.tcp_server.subscribers_table:
            return self.tcp_server.subscribers_table[topic]
        elif topic in self.tcp_server.ros_services_table:
            return self.tcp_server.ros_services_table[topic]
        elif topic in self.tcp_server.unity_services_table:
            return self.tcp_server.unity_services_table[topic]
        else:
            return None

    def send_topic_list(self):
        queue = self.queue
        if queue is not None:
            topic_list = SysCommand_TopicsResponse()
            topics_and_types = self.tcp_server.get_topic_names_and_types()
            topic_list.topics = [item[0] for item in topics_and_types]
            for i in topics_and_types:
                node = self.get_registered_topic(i[0])
                if len(i[1]) > 1:
                    if node is not None:
                        self.tcp_server.get_logger().warning(
                            "Only one message type per topic is supported, but found multiple types for topic {}; maintaining {} as the subscribed type.".format(
                                i[0], self.parse_message_name(node.msg)
                            )
                        )
                topic_list.types = [
                    item[1][0].replace("/msg/", "/")
                    if (len(item[1]) <= 1)
                    else self.parse_message_name(node.msg)
                    for item in topics_and_types
                ]
            serialized_bytes = ClientThread.serialize_command("__topic_list", topic_list)
            self._enqueue(queue, OutgoingFrame(serialized_bytes), "__topic_list")

    def claim_connection(self, conn):
        with self.queue_lock:
            if self.connection is not None:
                return None

            local_queue = OutgoingQueue(self.max_queued_messages, self.max_queued_bytes)
            local_queue.put(self.handshake)
            self.connection = conn
            self.queue = local_queue
            return local_queue

    def stop_sending(self, conn):
        with self.queue_lock:
            if conn is self.connection:
                self._close_queue()

    def _close_queue(self):
        if self.queue is not None:
            self.queue.close()
            self.queue = None
        for response in self.services_waiting.values():
            response.cancel()
        self.services_waiting.clear()

    @staticmethod
    def _shutdown_socket(conn):
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def release_connection(self, conn):
        with self.queue_lock:
            if conn is self.connection:
                self.connection = None

    def sender_loop(self, conn, halt_event, local_queue):
        try:
            while not halt_event.is_set():
                item = local_queue.get()
                if item is None or halt_event.is_set():
                    break
                if item.service_id is not None:
                    with self.queue_lock:
                        if (
                            local_queue is not self.queue
                            or item.service_id not in self.services_waiting
                            or monotonic() >= item.deadline
                        ):
                            continue
                conn.sendall(item.data)
        except OSError as error:
            if not halt_event.is_set():
                self.tcp_server.logwarn(f"Unity socket write failed: {error}")
                halt_event.set()
                self.stop_sending(conn)
                self._shutdown_socket(conn)

    def parse_message_name(self, name):
        try:
            # Example input string: <class 'std_msgs.msg._string.Metaclass_String'>
            names = (str(type(name))).split(".")
            module_name = names[0][8:]
            class_name = names[-1].split("_")[-1][:-2]
            return "{}/{}".format(module_name, class_name)
        except (IndexError, AttributeError, ImportError) as e:
            self.tcp_server.logerr("Failed to resolve message name: {}".format(e))
            return None


class SysCommand_Log:
    def __init__(self):
        text = ""


class SysCommand_Service:
    def __init__(self):
        srv_id = 0


class SysCommand_TopicsResponse:
    def __init__(self):
        topics = []
        types = []


class SysCommand_Handshake:
    def __init__(self, metadata):
        self.version = "v0.7.0"
        self.metadata = json.dumps(metadata.__dict__)


class SysCommand_Handshake_Metadata:
    def __init__(self):
        self.protocol = "ROS2"
