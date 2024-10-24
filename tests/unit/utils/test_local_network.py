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
import logging
import re
from socket import AddressFamily
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from hypothesis import given
from local_console.utils.local_network import get_mqtt_ip
from local_console.utils.local_network import get_my_ip_by_routing
from local_console.utils.local_network import get_network_ifaces
from local_console.utils.local_network import get_webserver_ip
from local_console.utils.local_network import is_localhost

# For some reason, pycln removes this import, but obviously
# pytest fails when running the tests!
from tests.fixtures.debugging import debug_log  # noreorder # noqa
from tests.strategies.configs import generate_text
from local_console.core.config import config_obj
from psutil._common import snicstats
from psutil._common import snicaddr

logger = logging.getLogger(__name__)


@pytest.fixture
def mock_psutil():
    mock = MagicMock()
    with patch("local_console.utils.local_network.psutil", mock):
        mock.net_if_stats.return_value = {
            "enp5s2": snicstats(True, 2, 0, 0, "up,broadcast,running,multicast")
        }
        mock.net_if_addrs.return_value = {
            "enp5s2": [
                snicaddr(
                    AddressFamily.AF_INET,
                    "192.168.1.123",
                    "255.255.255.0",
                    "192.168.1.255",
                    None,
                )
            ]
        }
        yield


def test_detect_interfaces(debug_log):
    interfaces = get_network_ifaces()

    assert "lo" not in interfaces
    assert all("docker" not in iface for iface in interfaces)
    assert all("ppp" not in iface for iface in interfaces)


def test_get_my_ip_by_routing(mock_psutil):
    # Ensure we get an IPv4 address
    local_ip = get_my_ip_by_routing()
    assert re.match(r"\d+\.\d+\.\d+\.\d+", local_ip)


def test_get_mqtt_ip_localhost(mock_psutil):
    config_obj.get_active_device_config().mqtt.host = "localhost"
    ip = get_mqtt_ip()
    assert ip != "localhost"
    assert ip == get_my_ip_by_routing()


def test_get_mqtt_ip(mock_psutil):
    config_obj.get_active_device_config().mqtt.host = "192.168.1.13"
    ip = get_mqtt_ip()
    assert ip == "192.168.1.13"


def test_get_webserver_ip_localhost(mock_psutil):
    config_obj.get_active_device_config().mqtt.host = "localhost"
    ip = get_webserver_ip()
    assert ip != "localhost"
    assert ip == get_my_ip_by_routing()


def test_get_webserver_ip(mock_psutil):
    config_obj.get_active_device_config().webserver.host = "192.168.1.13"
    ip = get_webserver_ip()
    assert ip == "192.168.1.13"


def test_is_localhost(mock_psutil):
    assert is_localhost("localhost")
    assert is_localhost("127.0.0.1")


@given(
    generate_text(),
)
def test_is_localhost_fail(hostname: str):
    assert not is_localhost("192.168.1.1.1")
    assert not is_localhost("192.168.1.1")
    assert not is_localhost(f"{hostname}.")
    assert not is_localhost("".join(map(str, range(10000))))
    with patch(
        "local_console.utils.local_network.socket.gethostbyname",
        side_effects=Exception,
    ):
        assert not is_localhost(hostname)
