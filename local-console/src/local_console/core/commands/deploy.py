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
import hashlib
import json
import logging
import uuid
from abc import ABC
from abc import abstractmethod
from collections.abc import Awaitable
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any
from typing import Callable
from typing import Optional

import trio
from local_console.core.camera.enums import DeployStage
from local_console.core.schemas.schemas import Deployment
from local_console.core.schemas.schemas import DeploymentManifest
from local_console.core.schemas.schemas import OnWireProtocol
from local_console.servers.webserver import SyncWebserver
from local_console.utils.local_network import get_webserver_ip
from local_console.utils.timing import TimeoutBehavior

logger = logging.getLogger(__name__)


class DeployFSM(ABC):
    def __init__(
        self,
        deploy_fn: Callable[[DeploymentManifest], Awaitable[None]],
        stage_callback: Optional[Callable[[DeployStage], Awaitable[None]]] = None,
        deploy_webserver: bool = True,
        webserver_port: int = 0,
        timeout_secs: int = 30,
    ) -> None:
        self.deploy_fn = deploy_fn
        self.stage_callback = stage_callback
        self.webserver = SyncWebserver(
            Path(), port=webserver_port, deploy=deploy_webserver
        )
        self.webserver.start()  # This secures a listening port for the webserver

        self.done = trio.Event()
        self._timeout_handler = TimeoutBehavior(timeout_secs, self._on_timeout)
        self._to_deploy: Optional[DeploymentManifest] = None
        self.errored: Optional[bool] = None

        self.stage: Optional[DeployStage] = None

    async def _set_new_stage(self, new_stage: DeployStage) -> None:
        self.stage = new_stage
        if self.stage_callback:
            await self.stage_callback(self.stage)

    @abstractmethod
    async def update(self, deploy_status: dict[str, Any]) -> None:
        """
        Updates the FSM as it progresses through its states.
        """
        assert self.stage
        assert self._to_deploy

    @abstractmethod
    async def start(self, nursery: trio.Nursery) -> None:
        """
        To be called for performing actions at FSM entry, once the
        deployment manifest is set. It must:
        - call spawn_in() of _timeout_handler
        - await _set_new_stage() with initial stage
        """

    def set_manifest(self, to_deploy: DeploymentManifest) -> None:
        self._to_deploy = to_deploy

    def stop(self) -> None:
        self._timeout_handler.stop()
        self.webserver.stop()
        self.done.set()

    async def check_termination(
        self, is_finished: bool, matches: bool, is_errored: bool
    ) -> bool:
        should_terminate = False
        next_stage = self.stage
        if matches:
            if is_finished:
                should_terminate = True
                next_stage = DeployStage.Done
                self.errored = False
                logger.info("Deployment complete")

            elif is_errored:
                should_terminate = True
                next_stage = DeployStage.Error
                self.errored = True
                logger.error("Deployment errored")

        if should_terminate:
            self.stop()
            assert next_stage
            await self._set_new_stage(next_stage)

        return should_terminate

    async def _on_timeout(self) -> None:
        logger.error("Timeout when sending modules.")
        self.errored = True
        self.stop()
        await self._set_new_stage(DeployStage.Error)

    @classmethod
    def instantiate(
        cls,
        onwire_schema: OnWireProtocol,
        deploy_fn: Callable[[DeploymentManifest], Awaitable[None]],
        stage_callback: Optional[Callable[[DeployStage], Awaitable[None]]] = None,
        deploy_webserver: bool = True,
        webserver_port_override: int = 0,
        timeout_secs: int = 30,
    ) -> "DeployFSM":
        # This is a factory builder, so only run this from this parent class
        assert cls is DeployFSM

        if onwire_schema == OnWireProtocol.EVP1:
            return EVP1DeployFSM(
                deploy_fn,
                stage_callback,
                deploy_webserver,
                webserver_port_override,
                timeout_secs,
            )
        elif onwire_schema == OnWireProtocol.EVP2:
            return EVP2DeployFSM(
                deploy_fn,
                stage_callback,
                deploy_webserver,
                webserver_port_override,
                timeout_secs,
            )


class EVP2DeployFSM(DeployFSM):

    async def start(self, nursery: trio.Nursery) -> None:
        """
        No further start actions required
        """
        assert self._to_deploy
        self._timeout_handler.spawn_in(nursery)
        await self._set_new_stage(DeployStage.WaitFirstStatus)

    async def update(self, deploy_status: dict[str, Any]) -> None:
        assert self._to_deploy
        assert self.stage
        next_stage = self.stage

        is_finished, matches, is_errored = verify_report(
            self._to_deploy.deployment.deploymentId, deploy_status
        )
        if await self.check_termination(is_finished, matches, is_errored):
            return

        if self.stage == DeployStage.WaitFirstStatus:
            logger.debug("Agent can receive deployments. Pushing manifest now.")
            await self.deploy_fn(self._to_deploy)
            next_stage = DeployStage.WaitAppliedConfirmation

        elif self.stage == DeployStage.WaitAppliedConfirmation:
            if matches:
                logger.debug(
                    "Deployment received, reconcile=%s",
                    deploy_status.get("reconcileStatus", "<null>"),
                )
                logger.info("Deployment received, waiting for reconcile completion")

        elif self.stage in (DeployStage.Done, DeployStage.Error):
            logger.warning(
                "Should not reach here! (status is %s)",
                json.dumps(deploy_status),
            )

        await self._set_new_stage(next_stage)


class EVP1DeployFSM(DeployFSM):

    async def start(self, nursery: trio.Nursery) -> None:
        assert self._to_deploy
        # Deploy immediately, without comparing with current status, to speed-up the process.
        logger.debug("Pushing manifest now.")
        self._timeout_handler.spawn_in(nursery)
        await self._set_new_stage(DeployStage.WaitAppliedConfirmation)
        await self.deploy_fn(self._to_deploy)

    async def update(self, deploy_status: dict[str, Any]) -> None:
        """
        Simplified handshake compared to EVP2:
        - Sends deployment at beginning.
        - Checks current is the one to be applied.
        """
        assert self._to_deploy
        is_finished, matches, is_errored = verify_report(
            self._to_deploy.deployment.deploymentId, deploy_status
        )
        if await self.check_termination(is_finished, matches, is_errored):
            return


def verify_report(
    deployment_id: str, deploy_status: dict[str, Any]
) -> tuple[bool, bool, bool]:
    matches = deploy_status.get("deploymentId") == deployment_id
    is_finished = deploy_status.get("reconcileStatus") == "ok"

    error_in_modules = any(
        n["status"] == "error" for n in deploy_status.get("modules", {}).values()
    )
    error_in_modinsts = any(
        n["status"] == "error" for n in deploy_status.get("instances", {}).values()
    )
    is_errored = error_in_modinsts or error_in_modules

    return is_finished, matches, is_errored


def single_module_manifest_setup(
    module_name: str,
    module_file: Path,
    webserver: SyncWebserver,
    port_override: Optional[int] = None,
    host_override: Optional[str] = None,
) -> DeploymentManifest:
    """
    Generate a single module, single instance deployment manifest,
    matched to the passed webserver instance.
    """
    deployment = Deployment.model_validate(
        {
            "deploymentId": "",
            "instanceSpecs": {
                module_name: {"moduleId": module_name, "subscribe": {}, "publish": {}}
            },
            "modules": {
                module_name: {
                    "entryPoint": "main",
                    "moduleImpl": "python",
                    "downloadUrl": "",
                    "hash": "",
                }
            },
            "publishTopics": {},
            "subscribeTopics": {},
        }
    )
    deployment.modules[module_name].downloadUrl = str(module_file)
    deployment_manifest = DeploymentManifest(deployment=deployment)

    webserver.set_directory(module_file.parent)
    return manifest_setup_epilog(
        module_file.parent, deployment_manifest, webserver, port_override, host_override
    )


def manifest_setup_epilog(
    files_dir: Path,
    manifest: DeploymentManifest,
    webserver: SyncWebserver,
    port_override: Optional[int] = None,
    host_override: Optional[str] = None,
) -> DeploymentManifest:
    """
    Fills in the URLs and hashes of a deployment manifest,
    and renames modules to ensure a module entry matches the
    hash of its target file (which ensures that subsequent
    versions of the same module won't be mistaken by the device's
    module cache as a cache hit.)
    """
    assert files_dir.is_dir()

    dm = manifest.copy(deep=True)
    host = get_webserver_ip() if not host_override else host_override
    port = webserver.port if not port_override else port_override
    populate_urls_and_hashes(dm, host, port, files_dir)
    make_unique_module_ids(dm)

    return dm


def make_unique_module_ids(deploy_man: DeploymentManifest) -> None:
    """
    Makes module identifiers in the deployment manifest unique
    across deployments, by suffixing them with a slice of the
    module's hash.
    """
    modules = deploy_man.deployment.modules
    old_to_new = {name: f"{name}-{m.hash[:5]}" for name, m in modules.items()}

    # Update modules
    for old_id, new_id in old_to_new.items():
        module = modules.pop(old_id)
        modules[new_id] = module

    # Update instanceSpecs
    instances = deploy_man.deployment.instanceSpecs
    for instance in instances.values():
        instance.moduleId = old_to_new[instance.moduleId]


def get_empty_deployment() -> DeploymentManifest:
    deployment = {
        "deployment": {
            "deploymentId": str(uuid.uuid4()),
            "instanceSpecs": {},
            "modules": {},
            "publishTopics": {},
            "subscribeTopics": {},
        }
    }
    return DeploymentManifest.model_validate(deployment)


def calculate_sha256(path: Path) -> str:
    sha256_hash = hashlib.sha256()
    sha256_hash.update(path.read_bytes())
    return sha256_hash.hexdigest()


def populate_urls_and_hashes(
    deployment_manifest: DeploymentManifest,
    host: str,
    port: int,
    root_path: Path,
) -> None:
    for module in deployment_manifest.deployment.modules.keys():
        file = Path(deployment_manifest.deployment.modules[module].downloadUrl)
        deployment_manifest.deployment.modules[module].hash = calculate_sha256(file)
        url = f"http://{host}:{port}/{PurePosixPath(file.relative_to(root_path))}"
        deployment_manifest.deployment.modules[module].downloadUrl = url

    # DeploymentId based on deployment manifest content
    deployment_manifest_hash = hashlib.sha256(
        str(deployment_manifest.model_dump()).encode("utf-8")
    )
    deployment_manifest.deployment.deploymentId = deployment_manifest_hash.hexdigest()


def deploy_status_empty(deploy_status: Optional[dict[str, Any]]) -> bool:
    if not deploy_status:
        return True

    if "reconcileStatus" not in deploy_status:
        return True

    if "deploymentId" not in deploy_status:
        return True

    return False
