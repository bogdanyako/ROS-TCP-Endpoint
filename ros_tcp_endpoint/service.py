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

import rclpy
import re
from threading import Event

from rclpy.serialization import deserialize_message

from .communication import RosSender


class RosService(RosSender):
    """
    Class to send messages to a ROS service.
    """

    def __init__(self, service, service_class):
        """
        Args:
            service:        The service name in ROS
            service_class:  The service class in catkin workspace
        """
        strippedService = re.sub("[^A-Za-z0-9_]+", "", service)
        node_name = f"{strippedService}_RosService"
        RosSender.__init__(self, node_name)

        self.service_topic = service
        self.cli = self.create_client(service_class, service)
        self.service_class = service_class

    def send(self, data, halt_event):
        """
        Takes in serialized message data from source outside of the ROS network,
        deserializes it into it's class, calls the service with the message, and returns
        the service's response.

        Args:
            data: The already serialized message_class data coming from outside of ROS
            halt_event: Signals that the requesting Unity connection closed

        Returns:
            service response
        """
        message = deserialize_message(data, self.service_class.Request)

        if not self.cli.service_is_ready():
            self.get_logger().error(
                "Ignoring service call to {} - service is not ready.".format(self.service_topic)
            )
            return None

        if halt_event.is_set():
            return None
        future = self.cli.call_async(message)
        completed = Event()
        future.add_done_callback(lambda _: completed.set())
        while not halt_event.is_set() and rclpy.ok():
            if completed.wait(0.05):
                try:
                    return future.result()
                except Exception as error:
                    self.get_logger().info(f"Service call failed {error}")
                    return None

        self.cli.remove_pending_request(future)
        future.cancel()
        return None

    def unregister(self):
        """

        Returns:

        """
        self.destroy_client(self.cli)
        self.destroy_node()
