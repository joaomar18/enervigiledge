###########EXTERNAL IMPORTS############

import json
from fastapi import FastAPI
from fastapi.testclient import TestClient

#######################################

#############LOCAL IMPORTS#############

from web import nodes_api, auth_api
from web.dependencies import services
from web.safety import HTTPSafety
from controller.node import Node
from controller.types import NodeType, NodeConfig
from argon2 import PasswordHasher

#######################################


class DummyDevice:
    def __init__(self, id, name, nodes):
        self.id = id
        self.name = name
        self.nodes = nodes


class DummyDeviceManager:
    def __init__(self, devices):
        self.devices = devices

    def get_device(self, device_id):
        for d in self.devices:
            if d.id == device_id:
                return d
        return None


class DummyTimeDB:
    def get_measurement_data_between(self, device_name, device_id, measurement, start_time=None, end_time=None):
        return [{"time": "0", "value": 1}]


def create_app(safety, device_manager, timedb):
    app = FastAPI()
    services.set_dependencies(safety, device_manager, object(), timedb)
    app.include_router(auth_api.router)
    app.include_router(nodes_api.router)
    return app


def test_nodes_endpoints_use_query_params(tmp_path):
    config = {"username": "user", "password_hash": PasswordHasher().hash("secret"), "jwt_secret": "secretkey"}
    config_path = tmp_path / "user_config.json"
    config_path.write_text(json.dumps(config))
    HTTPSafety.USER_CONFIG_PATH = str(config_path)

    safety = HTTPSafety()
    node = Node(NodeConfig("voltage", NodeType.FLOAT, "V"))
    node.set_value(10)
    device = DummyDevice(1, "dev1", [node])
    device_manager = DummyDeviceManager([device])
    timedb = DummyTimeDB()

    app = create_app(safety, device_manager, timedb)

    with TestClient(app) as client:
        response = client.post("/auth/login", json={"username": "user", "password": "secret"})
        assert response.status_code == 200
        token = response.cookies.get("token")
        headers = {"Authorization": f"Bearer {token}"}

        state_resp = client.get("/nodes/get_nodes_state", params={"id": 1}, headers=headers)
        assert state_resp.status_code == 200
        logs_resp = client.get("/nodes/get_logs_from_node", params={"id": 1, "node": "voltage"}, headers=headers)
        assert logs_resp.status_code == 200
