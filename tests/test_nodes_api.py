########### EXTERNAL IMPORTS ############

import asyncio
import json
import types
from typing import Set, List
from fastapi import FastAPI
from fastapi.testclient import TestClient
from argon2 import PasswordHasher

#########################################

############# LOCAL IMPORTS #############

from web.api import nodes, auth
from web.dependencies import services
from web.safety import HTTPSafety
from controller.node.node import Node
from controller.meter.device import EnergyMeter
from model.controller.node import NodeType, NodeConfig, BaseNodeProtocolOptions, NodeLogs
from model.controller.general import Protocol
from model.controller.device import BaseCommunicationOptions, EnergyMeterType, EnergyMeterOptions, EnergyMeterRecord

#########################################


class DummyMeter(EnergyMeter):
    def __init__(self, nodes: Set[Node]):
        self.name = "meter"
        self.id = 1
        self.publish_queue = asyncio.Queue()
        self.measurements_queue = asyncio.Queue()
        self.meter_nodes = types.SimpleNamespace(nodes={n.config.name: n for n in nodes})
        self.protocol = Protocol.MODBUS_RTU
        self.connected = True
        self.meter_type = EnergyMeterType.THREE_PHASE
        self.meter_options = EnergyMeterOptions()
        self.communication_options = BaseCommunicationOptions()

    async def start(self):
        pass

    async def stop(self):
        pass

    def get_meter_record(self) -> EnergyMeterRecord:
        return EnergyMeterRecord(
            id=self.id,
            name=self.name,
            protocol=Protocol.MODBUS_RTU,  # Dummy protocol
            type=EnergyMeterType.THREE_PHASE,  # Dummy type
            options=EnergyMeterOptions(),  # Dummy options
            communication_options=BaseCommunicationOptions(),  # Dummy connection options
            nodes=set(),  # Dummy nodes
        )


class DummyDeviceManager:
    def __init__(self, devices: List[DummyMeter]):
        self.devices = devices

    def get_device(self, device_id: int):
        for d in self.devices:
            if d.id == device_id:
                return d
        return None


class DummyTimeDB:
    def get_variable_logs(self, device_name, device_id, measurement, start_time=None, end_time=None) -> NodeLogs:
        return NodeLogs(unit="V", decimal_places=2, type=NodeType.FLOAT, is_counter=False, points=[{"time": "0", "value": 1}], time_step=None, global_metrics=None)


class DummySQLiteDB:
    pass


def create_app(safety, device_manager, sqlitedb, timedb):
    app = FastAPI()
    services.set_dependencies(safety, device_manager, sqlitedb, timedb)
    app.include_router(auth.router)
    app.include_router(nodes.router)
    return app


def test_nodes_endpoints_use_query_params(tmp_path):
    # Setup config for HTTPSafety
    config = {
        "username": "user",
        "password_hash": PasswordHasher().hash("secret"),
        "jwt_secret": "secretkey",
    }
    config_path = tmp_path / "user_config.json"
    config_path.write_text(json.dumps(config))
    HTTPSafety.USER_CONFIG_PATH = str(config_path)

    # Dependencies
    safety = HTTPSafety()
    node = Node(NodeConfig("voltage", NodeType.FLOAT, "V"), BaseNodeProtocolOptions())
    node.processor.set_value(10)
    meter = DummyMeter({node})
    device_manager = DummyDeviceManager([meter])
    timedb = DummyTimeDB()
    sqlitedb = DummySQLiteDB()

    app = create_app(safety, device_manager, sqlitedb, timedb)

    with TestClient(app) as client:
        # 1) Login
        resp = client.post("/auth/login", json={"username": "user", "password": "secret"})
        assert resp.status_code == 200

        # 2) Get token (prefer JSON; fallback to cookie)
        token = resp.json().get("token") or resp.cookies.get("token")
        assert token, "Login must return a token in JSON or cookie"

        headers = {"Authorization": f"Bearer {token}"}

        # 3) Call endpoints with header
        state_resp = client.get("/nodes/get_nodes_state", params={"id": 1}, headers=headers)
        if state_resp.status_code != 200:
            print("Error response:", state_resp.json())  # Debugging output
        assert state_resp.status_code == 200

        logs_resp = client.get("/nodes/get_logs_from_node", params={"id": 1, "node_name": "voltage"}, headers=headers)
        if logs_resp.status_code != 200:
            print("Error response:", logs_resp.json())  # Debugging output
        assert logs_resp.status_code == 200