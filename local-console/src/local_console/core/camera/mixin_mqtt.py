# Copyright 2024 Sony Semiconductor Solutions Corp.
#
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
#
# SPDX-License-Identifier: Apache-2.0
import json
import logging
from base64 import b64decode
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import Optional
from typing import Protocol

import trio
from exceptiongroup import ExceptionGroup
from local_console.clients.agent import Agent
from local_console.clients.agent import check_attributes_request
from local_console.core.camera._shared import IsAsyncReady
from local_console.core.camera.enums import MQTTTopics
from local_console.core.camera.enums import StreamStatus
from local_console.core.schemas.edge_cloud_if_v1 import DeviceConfiguration
from local_console.core.schemas.edge_cloud_if_v1 import Permission
from local_console.core.schemas.edge_cloud_if_v1 import SetFactoryReset
from local_console.core.schemas.schemas import DeviceConnection
from local_console.core.schemas.schemas import OnWireProtocol
from local_console.servers.broker import BrokerException
from local_console.servers.broker import spawn_broker
from local_console.utils.timing import TimeoutBehavior
from local_console.utils.tracking import TrackingVariable
from pydantic import ValidationError
from trio import TASK_STATUS_IGNORED

logger = logging.getLogger(__name__)


# MQTT constants
EA_STATE_TOPIC = "state/node/placeholder"
SYSINFO_TOPIC = "systemInfo"
DEPLOY_STATUS_TOPIC = "deploymentStatus"
CONNECTION_STATUS_TIMEOUT = timedelta(seconds=180)


class HoldsDeployStatus(Protocol):
    """
    This Protocol states that classes onto which this applies,
    will have a `deploy_status` member. For MQTTMixin below,
    this means that `deploy_status` originates elsewhere within
    CameraState, but MQTTMixing expects to find it.
    """

    deploy_status: TrackingVariable[dict[str, str]]


class CanStopStreaming(Protocol):
    """
    This Protocol states that classes onto which this applies,
    will have a `streaming_rpc_stop` member. For MQTTMixin below,
    this means that `streaming_rpc_stop` originates elsewhere within
    CameraState, but MQTTMixing expects to find it.
    """

    async def streaming_rpc_stop(self) -> None: ...


class MQTTMixin(HoldsDeployStatus, CanStopStreaming, IsAsyncReady):
    """
    This Mix-in class covers the MQTT management concern belonging
    to a camera's state. This includes handling the MQTT broker and
    the client used by local-console to interact to the camera.
    """

    def __init__(self) -> None:

        # Ancillary variables
        self.mqtt_client: Optional[Agent] = None
        self._onwire_schema: Optional[OnWireProtocol] = None
        self.timeouts: dict[str, TimeoutBehavior] = {}
        self._last_reception: Optional[datetime] = None
        self._ota_event = trio.Event()

        # State variables
        self.device_config: TrackingVariable[DeviceConfiguration] = TrackingVariable()
        self.attributes_available: TrackingVariable[bool] = TrackingVariable(False)
        self.stream_status: TrackingVariable[StreamStatus] = TrackingVariable(
            StreamStatus.Inactive
        )
        self.is_streaming: TrackingVariable[bool] = TrackingVariable(False)
        self.is_connected: TrackingVariable[bool] = TrackingVariable(False)
        self.is_ready: TrackingVariable[bool] = TrackingVariable(False)

        self.mqtt_host: TrackingVariable[str] = TrackingVariable("")
        self.mqtt_port: TrackingVariable[int] = TrackingVariable()
        self.ntp_host: TrackingVariable[str] = TrackingVariable("")
        self.ip_address: TrackingVariable[str] = TrackingVariable("")
        self.subnet_mask: TrackingVariable[str] = TrackingVariable("")
        self.gateway: TrackingVariable[str] = TrackingVariable("")
        self.dns_server: TrackingVariable[str] = TrackingVariable("")
        self.wifi_ssid: TrackingVariable[str] = TrackingVariable("")
        self.wifi_password: TrackingVariable[str] = TrackingVariable("")

    def _setup_timeouts(self, nursery: trio.Nursery) -> None:
        assert self._onwire_schema

        # This takes care of ensuring the device reports its state
        # with bounded periodicity (expect to receive a message within 6 seconds)
        if self._onwire_schema == OnWireProtocol.EVP2:
            self.timeouts["periodic-reports"] = TimeoutBehavior(
                6, self.set_periodic_reports
            )

        # This timeout behavior takes care of updating the connectivity
        # status in case there are no incoming messages from the camera
        # for longer than the threshold
        self.timeouts["connection-alive"] = TimeoutBehavior(
            CONNECTION_STATUS_TIMEOUT.seconds,
            self.connection_status_timeout,
        )
        self.timeouts["connection-alive"].spawn_in(nursery)

    def _init_bindings_mqtt(self) -> None:
        """
        These bindings among variables implement business logic that requires
        no further data than the one contained among the variables.
        """

        def compute_is_ready(current: Optional[bool], previous: Optional[bool]) -> None:
            # Attributes report interval cannot be controlled in EVP1
            assert self._onwire_schema
            _is_ready = (
                False
                if current is None
                else (current and (self._onwire_schema is not None))
            )
            self.is_ready.value = _is_ready

        self.attributes_available.subscribe(compute_is_ready)

        def compute_is_streaming(
            current: Optional[StreamStatus], previous: Optional[StreamStatus]
        ) -> None:
            _is_streaming = (
                False if current is None else (current == StreamStatus.Active)
            )
            self.is_streaming.value = _is_streaming

        self.stream_status.subscribe(compute_is_streaming)

        async def prepare_ota_event(
            current: Optional[DeviceConfiguration],
            previous: Optional[DeviceConfiguration],
        ) -> None:
            if current != previous:
                self._ota_event.set()

        self.device_config.subscribe_async(prepare_ota_event)

        self.device_config.subscribe_async(self.process_factory_reset)

    def initialize_connection_variables(
        self, iot_platform: str, config: DeviceConnection
    ) -> None:
        self.mqtt_host.value = config.mqtt.host
        self.mqtt_port.value = int(config.mqtt.port)
        self._onwire_schema = OnWireProtocol.from_iot_spec(iot_platform)
        self.ntp_host.value = "pool.ntp.org"

    async def mqtt_setup(self, *, task_status: Any = TASK_STATUS_IGNORED) -> None:
        assert self.mqtt_client is None
        assert self.mqtt_host.value
        assert self.mqtt_port.value
        assert self._onwire_schema

        port = self.mqtt_port.value

        self.mqtt_client = Agent(self.mqtt_host.value, port, self._onwire_schema)
        try:
            async with (
                trio.open_nursery() as nursery,
                spawn_broker(port, nursery, False),
                self.mqtt_client.mqtt_scope(
                    [
                        MQTTTopics.ATTRIBUTES_REQ.value,
                        MQTTTopics.TELEMETRY.value,
                        MQTTTopics.RPC_RESPONSES.value,
                        MQTTTopics.ATTRIBUTES.value,
                    ]
                ),
            ):
                assert self.mqtt_client.client
                self._setup_timeouts(nursery)

                task_status.started(True)
                streaming_stop_required = True
                async with self.mqtt_client.client.messages() as mgen:
                    async for msg in mgen:
                        if await check_attributes_request(
                            self.mqtt_client, msg.topic, msg.payload.decode()
                        ):
                            self.attributes_available.value = True
                            # attributes request handshake is performed at (re)connect
                            # when reconnecting, multiple requests might be made
                            if streaming_stop_required:
                                await self.streaming_rpc_stop()
                                streaming_stop_required = False

                        payload = json.loads(msg.payload)
                        await self.process_incoming(msg.topic, payload)

                        self.update_connection_status()
                        if self._onwire_schema == OnWireProtocol.EVP2 and self.is_ready:
                            self.timeouts["periodic-reports"].tap()

        except ExceptionGroup as exc_grp:
            task_status.started(False)
            for e in exc_grp.exceptions:
                if isinstance(e, BrokerException):
                    await self.message_send_channel.send(("error", str(e)))

    async def set_periodic_reports(self) -> None:
        # Configure the device to emit status reports twice
        # as often as the timeout expiration, to avoid that
        # random deviations in reporting periodicity make the timer
        # to expire unnecessarily.
        assert self.mqtt_client
        assert self._onwire_schema == OnWireProtocol.EVP2
        assert "periodic-reports" in self.timeouts

        timeout = int(0.5 * self.timeouts["periodic-reports"].timeout_secs)
        await self.mqtt_client.set_periodic_reports(timeout)

    def _check_connection_status(self) -> bool:
        if self._last_reception is None:
            return False
        else:
            return (datetime.now() - self._last_reception) < CONNECTION_STATUS_TIMEOUT

    def update_connection_status(self) -> None:
        self.is_connected.value = self._check_connection_status()
        if self.is_connected.value:
            self.timeouts["connection-alive"].tap()

    async def connection_status_timeout(self) -> None:
        logger.debug("Connection status timed out: camera is disconnected")
        self.stream_status.value = StreamStatus.Inactive
        self.update_connection_status()

    async def process_incoming(self, topic: str, payload: dict[str, Any]) -> None:
        sent_from_camera = False
        if topic == MQTTTopics.ATTRIBUTES.value:
            if EA_STATE_TOPIC in payload:
                sent_from_camera = True
                await self._process_state_topic(payload)

            if SYSINFO_TOPIC in payload:
                sent_from_camera = True
                await self._process_sysinfo_topic(payload)

            if DEPLOY_STATUS_TOPIC in payload:
                sent_from_camera = True
                await self._process_deploy_status_topic(payload)

        if topic == MQTTTopics.TELEMETRY.value:
            sent_from_camera = True

        if sent_from_camera:
            self._last_reception = datetime.now()
            logger.debug("Incoming on %s: %s", topic, str(payload))

    async def _process_state_topic(self, payload: dict[str, Any]) -> None:
        firmware_is_supported = False
        try:
            decoded = json.loads(b64decode(payload[EA_STATE_TOPIC]))
            firmware_is_supported = True
            self.attributes_available.value = True
        except UnicodeDecodeError:
            decoded = json.loads(payload[EA_STATE_TOPIC])

        if firmware_is_supported:
            try:
                await self.device_config.aset(
                    DeviceConfiguration.model_validate(decoded)
                )
                if self.device_config.value:
                    self.stream_status.value = StreamStatus.from_string(
                        self.device_config.value.Status.Sensor
                    )
            except ValidationError as e:
                logger.warning(f"Error while validating device configuration: {e}")

    async def _process_sysinfo_topic(self, payload: dict[str, Any]) -> None:
        sys_info = payload[SYSINFO_TOPIC]
        if "protocolVersion" in sys_info:
            self._onwire_schema = OnWireProtocol(sys_info["protocolVersion"])
        self.attributes_available.value = True

    async def _process_deploy_status_topic(self, payload: dict[str, Any]) -> None:
        if self._onwire_schema == OnWireProtocol.EVP1 or self._onwire_schema is None:
            update = json.loads(payload[DEPLOY_STATUS_TOPIC])
        else:
            update = payload[DEPLOY_STATUS_TOPIC]

        self.attributes_available.value = True
        await self.deploy_status.aset(update)

    async def process_factory_reset(
        self,
        current: Optional[DeviceConfiguration],
        previous: Optional[DeviceConfiguration],
    ) -> None:
        assert current
        assert self.mqtt_client

        factory_reset = current.Permission.FactoryReset
        logger.debug(f"Factory Reset is {factory_reset}")
        if not factory_reset:
            await self.mqtt_client.configure(
                "backdoor-EA_Main",
                "placeholder",
                SetFactoryReset(
                    Permission=Permission(FactoryReset=True)
                ).model_dump_json(),
            )

    async def ota_event(self) -> None:
        self._ota_event = trio.Event()
        await self._ota_event.wait()
