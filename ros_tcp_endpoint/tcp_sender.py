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
from queue import Queue

from rclpy.serialization import deserialize_message

from .client import ClientThread
from .exceptions import UnityServiceCallError


class UnityTcpSender:
    """
    Sends messages to Unity.
    """

    def __init__(self, tcp_server, service_timeout_seconds):
        if not math.isfinite(service_timeout_seconds) or service_timeout_seconds <= 0:
            raise ValueError("UNITY_SERVICE_TIMEOUT_SECONDS must be finite and positive")

        self.tcp_server = tcp_server
        self.service_timeout_seconds = service_timeout_seconds

        # The lock protects the active connection, its queue, and pending service calls.
        self.connection = None
        self.queue = None
        self.queue_lock = threading.Lock()

        self.next_srv_id = 1001
        self.services_waiting = {}

    def send_unity_info(self, text):
        queue = self.queue
        if queue is not None:
            command = SysCommand_Log()
            command.text = text
            serialized_bytes = ClientThread.serialize_command("__log", command)
            queue.put(serialized_bytes)

    def send_unity_warning(self, text):
        queue = self.queue
        if queue is not None:
            command = SysCommand_Log()
            command.text = text
            serialized_bytes = ClientThread.serialize_command("__warn", command)
            queue.put(serialized_bytes)

    def send_unity_error(self, text, conn=None):
        command = SysCommand_Log()
        command.text = text
        frame = ClientThread.serialize_command("__error", command)
        with self.queue_lock:
            if self.queue is not None and (conn is None or conn is self.connection):
                self.queue.put(frame)

    def send_ros_service_response(self, conn, srv_id, destination, response):
        command = SysCommand_Service()
        command.srv_id = srv_id
        header = ClientThread.serialize_command("__response", command)
        payload = ClientThread.serialize_message(destination, response)
        with self.queue_lock:
            if conn is self.connection and self.queue is not None:
                self.queue.put(header + payload)

    def send_unity_message(self, topic, message):
        queue = self.queue
        if queue is not None:
            serialized_message = ClientThread.serialize_message(topic, message)
            queue.put(serialized_message)

    def send_unity_service_request(self, topic, service_class, request):
        serialized_message = ClientThread.serialize_message(topic, request)
        response = Future()
        with self.queue_lock:
            if self.queue is None:
                raise UnityServiceCallError("Unity is disconnected")
            srv_id = self.next_srv_id
            self.next_srv_id += 1
            command = SysCommand_Service()
            command.srv_id = srv_id
            serialized_header = ClientThread.serialize_command("__request", command)
            self.services_waiting[srv_id] = response
            self.queue.put(serialized_header + serialized_message)

        try:
            data = response.result(timeout=self.service_timeout_seconds)
        except TimeoutError as error:
            raise UnityServiceCallError(
                f"Unity did not respond within {self.service_timeout_seconds:g} seconds"
            ) from error
        except CancelledError as error:
            raise UnityServiceCallError("Unity disconnected while the request was pending") from error
        finally:
            with self.queue_lock:
                self.services_waiting.pop(srv_id, None)

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
            queue.put(serialized_bytes)

    def claim_connection(self, conn):
        with self.queue_lock:
            if self.connection is not None:
                return None

            local_queue = Queue()
            handshake = SysCommand_Handshake(SysCommand_Handshake_Metadata())
            local_queue.put(ClientThread.serialize_command("__handshake", handshake))
            self.connection = conn
            self.queue = local_queue
            return local_queue

    def stop_sending(self, conn):
        with self.queue_lock:
            if conn is self.connection:
                if self.queue is not None:
                    self.queue.put(None)
                self.queue = None
                self._cancel_pending_services()

    def release_connection(self, conn):
        with self.queue_lock:
            if conn is self.connection:
                self.connection = None

    def _cancel_pending_services(self):
        for response in self.services_waiting.values():
            response.cancel()
        self.services_waiting.clear()

    def sender_loop(self, conn, halt_event, local_queue):
        try:
            while not halt_event.is_set():
                item = local_queue.get()
                if item is None or halt_event.is_set():
                    break
                conn.sendall(item)
        except OSError as error:
            if not halt_event.is_set():
                self.tcp_server.logwarn(f"Unity socket write failed: {error}")
                halt_event.set()
                self.stop_sending(conn)
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

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
