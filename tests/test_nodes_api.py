########### EXTERNAL IMPORTS ############

import json
from fastapi import FastAPI
from fastapi.testclient import TestClient
from argon2 import PasswordHasher

#########################################

############# LOCAL IMPORTS #############

from web.api import nodes, auth
from web.dependencies import services
from web.safety import HTTPSafety
from controller.node.node import Node
from model.controller.node import NodeType, NodeConfig

#########################################


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
    node = Node(NodeConfig("voltage", NodeType.FLOAT, "V"))
    node.processor.set_value(10)
    device = DummyDevice(1, "dev1", [node])
    device_manager = DummyDeviceManager([device])
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
        assert state_resp.status_code == 200

        logs_resp = client.get("/nodes/get_logs_from_node", params={"id": 1, "node": "voltage"}, headers=headers)
        assert logs_resp.status_code == 200
