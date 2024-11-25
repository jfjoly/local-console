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
import shutil
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional
from typing import Protocol

import trio
from local_console.clients.agent import Agent
from local_console.core.camera._shared import IsAsyncReady
from local_console.core.camera.axis_mapping import pixel_roi_from_normals
from local_console.core.camera.axis_mapping import UnitROI
from local_console.core.camera.flatbuffers import add_class_names
from local_console.core.camera.flatbuffers import flatbuffer_binary_to_json
from local_console.core.camera.flatbuffers import FlatbufferError
from local_console.core.camera.flatbuffers import get_output_from_inference_results
from local_console.core.camera.streaming import FileGrouping
from local_console.core.schemas.edge_cloud_if_v1 import StartUploadInferenceData
from local_console.gui.drawer.classification import ClassificationDrawer
from local_console.gui.drawer.objectdetection import DetectionDrawer
from local_console.gui.enums import ApplicationType
from local_console.gui.utils.sync_async import run_on_ui_thread
from local_console.servers.webserver import AsyncWebserver
from local_console.utils.fstools import check_and_create_directory
from local_console.utils.fstools import DirectoryMonitor
from local_console.utils.fstools import StorageSizeWatcher
from local_console.utils.local_network import get_webserver_ip
from local_console.utils.tracking import TrackingVariable


logger = logging.getLogger(__name__)


# MQTT constants
EA_STATE_TOPIC = "state/node/placeholder"
SYSINFO_TOPIC = "systemInfo"
DEPLOY_STATUS_TOPIC = "deploymentStatus"
CONNECTION_STATUS_TIMEOUT = timedelta(seconds=180)


class HasMQTTset(Protocol):
    """
    This Protocol states that classes onto which this applies,
    will have a `mqtt_client` member. For StreamingMixin below,
    this means that `mqtt_client` originates elsewhere within
    CameraState, but StreamingMixin expects to find it.
    """

    mqtt_client: Optional[Agent]


class StreamingMixin(HasMQTTset, IsAsyncReady):
    """
    This Mix-in class covers the concerns of streaming status and control
    belonging to a camera's state. This includes the start/stop commands
    for streaming and inference, and managing the webserver that supports
    the associated data traffic.
    """

    def __init__(self) -> None:

        # Ancillary variables
        self.upload_port: int | None = None
        self._extension_images = "jpg"
        self._extension_infers = "txt"
        self._grouper = FileGrouping({self._extension_images, self._extension_infers})
        self.total_dir_watcher = StorageSizeWatcher()
        self.dir_monitor = DirectoryMonitor()

        # State variables
        self.stream_image: TrackingVariable[str] = TrackingVariable("")
        self.image_dir_path: TrackingVariable[Path] = TrackingVariable()
        self.roi: TrackingVariable[UnitROI] = TrackingVariable()

        self.inference_field: TrackingVariable[str] = TrackingVariable("")
        self.inference_dir_path: TrackingVariable[Path] = TrackingVariable()

        self.size: TrackingVariable[str] = TrackingVariable("10")
        self.unit: TrackingVariable[str] = TrackingVariable("MB")

        self.vapp_schema_file: TrackingVariable[str] = TrackingVariable("")
        self.vapp_config_file: TrackingVariable[str] = TrackingVariable("")
        self.vapp_labels_file: TrackingVariable[str] = TrackingVariable("")
        self.vapp_type: TrackingVariable[str] = TrackingVariable(
            ApplicationType.CUSTOM.value
        )
        self.vapp_labels_map: TrackingVariable[dict[int, str]] = TrackingVariable()

    def _init_bindings_streaming(self) -> None:
        """
        These bindings among variables implement business logic that requires
        no further data than the one contained among the variables.
        """
        self.image_dir_path.subscribe(self.input_directory_setup)
        self.inference_dir_path.subscribe(self.input_directory_setup)

    async def streaming_rpc_stop(self) -> None:
        assert self.mqtt_client

        instance_id = "node"
        method = "StopUploadInferenceData"
        await self.mqtt_client.rpc(instance_id, method, "{}")

    async def streaming_rpc_start(self, roi: Optional[UnitROI] = None) -> None:
        assert self.mqtt_client

        instance_id = "node"
        method = "StartUploadInferenceData"
        host = get_webserver_ip()
        upload_url = f"http://{host}:{self.upload_port}"

        (h_offset, v_offset), (h_size, v_size) = pixel_roi_from_normals(roi)

        await self.mqtt_client.rpc(
            instance_id,
            method,
            StartUploadInferenceData(
                StorageName=upload_url,
                StorageSubDirectoryPath="images",
                StorageNameIR=upload_url,
                StorageSubDirectoryPathIR="inferences",
                CropHOffset=h_offset,
                CropVOffset=v_offset,
                CropHSize=h_size,
                CropVSize=v_size,
            ).model_dump_json(),
        )

    async def blobs_webserver_task(self) -> None:
        """
        Spawn a webserver on an arbitrary available port for receiving
        images from a camera.
        :param on_received: Callback that is triggered for each new received image
        :param base_dir: Path to directory where images will be saved into
        :return:
        """
        with (TemporaryDirectory(prefix="LocalConsole_") as tempdir,):
            logger.info(f"Webserver_task {str(tempdir)}")
            async with AsyncWebserver(
                Path(tempdir), port=0, on_incoming=self._process_camera_upload
            ) as image_serve:

                logger.info(f"Uploading data into {tempdir}")

                assert image_serve.port
                self.upload_port = image_serve.port
                logger.info(f"Webserver listening on port {self.upload_port}")
                tmp_image_directory = Path(tempdir) / "images"
                tmp_inference_directory = Path(tempdir) / "inferences"
                tmp_image_directory.mkdir(exist_ok=True)
                tmp_inference_directory.mkdir(exist_ok=True)

                if not self.image_dir_path.value:
                    self.image_dir_path.value = tmp_image_directory
                if not self.inference_dir_path.value:
                    self.inference_dir_path.value = tmp_inference_directory

                await trio.sleep_forever()

    @run_on_ui_thread
    def _process_camera_upload(self, incoming_file: Path) -> None:
        if incoming_file.suffix.lstrip(".") == self._extension_infers:
            assert self.inference_dir_path.value
            target_dir = Path(self.inference_dir_path.value)
            assert target_dir
            final_file = self._save_into_input_directory(incoming_file, target_dir)
            logger.debug(f"Inferences file path : {final_file}")
            self._grouper.register(final_file, final_file)

        elif incoming_file.suffix.lstrip(".") == self._extension_images:
            assert self.image_dir_path.value
            target_dir = Path(self.image_dir_path.value)
            assert target_dir
            final_file = self._save_into_input_directory(incoming_file, target_dir)
            logger.debug(f"Images file path : {final_file}")
            self._grouper.register(final_file, final_file)
        else:
            logger.warning(f"Unknown incoming file: {incoming_file}")

        # Process matched image-inference file pairs
        for pair in self._grouper:
            inference_file = pair[self._extension_infers]
            image_file = pair[self._extension_images]

            payload_render = inference_file.read_text()
            output_data = get_output_from_inference_results(inference_file.read_bytes())
            if self.vapp_schema_file.value:
                try:
                    output_tensor = self._get_flatbuffers_inference_data(output_data)
                    if output_tensor:
                        payload_render = json.dumps(output_tensor, indent=2)
                        output_data = output_tensor  # type: ignore
                except FlatbufferError as e:
                    logger.error("Error decoding inference data:", exc_info=e)

            self.inference_field.value = payload_render
            try:
                {
                    ApplicationType.CLASSIFICATION.value: ClassificationDrawer,
                    ApplicationType.DETECTION.value: DetectionDrawer,
                }[str(self.vapp_type.value)].process_frame(image_file, output_data)
                # Adding drawings modifies file size. Update storage watcher
                self.total_dir_watcher.update_file_size(image_file)
            except Exception as e:
                logger.error(f"Error while performing the drawing: {e}")
            self.stream_image.value = str(image_file)

    def input_directory_setup(
        self, current: Optional[str], previous: Optional[str]
    ) -> None:
        cur_path = Path(current) if isinstance(current, str) else current
        pre_path = Path(previous) if isinstance(previous, str) else previous

        if pre_path:
            self.total_dir_watcher.unwatch_path(pre_path)
        if cur_path:
            check_and_create_directory(cur_path)
            self.total_dir_watcher.set_path(cur_path)
            self.dir_monitor.watch(cur_path, self.notify_directory_deleted)

        if pre_path:
            self.dir_monitor.unwatch(pre_path)

    def notify_directory_deleted(self, dir_path: Path) -> None:
        self.send_message_sync("error", f"Directory {dir_path} does no longer exist.")

    def _save_into_input_directory(self, incoming_file: Path, target_dir: Path) -> Path:
        assert incoming_file.is_file()

        """
        The following cannot be asserted in the current implementation
        based on temporary directories, because of unexpected OS deletion
        of the target directory if it hasn't been set to the default
        temporary directory:

        assert target_dir.is_dir()
        """

        final = incoming_file
        check_and_create_directory(final.parent)
        if incoming_file.parent != target_dir:
            logger.debug("Moving file to def path")
            check_and_create_directory(target_dir)
            target_file = target_dir.joinpath(incoming_file.name)
            if target_file.exists():
                logger.info("Image with same name has arrived. Removing previous one.")
                target_file.unlink()
            final = Path(shutil.move(incoming_file, target_dir))
        self.total_dir_watcher.incoming(final)
        return final

    def _get_flatbuffers_inference_data(
        self, flatbuffer_payload: bytes
    ) -> None | str | dict:
        return_value = None
        if self.vapp_schema_file.value:
            json_data = flatbuffer_binary_to_json(
                self.vapp_schema_file.value, flatbuffer_payload
            )
            labels_map = self.vapp_labels_map.value
            if labels_map:
                add_class_names(json_data, labels_map)
            return_value = json_data

        return return_value
