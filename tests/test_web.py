###########EXTERNAL IMPORTS############

import json
from fastapi import FastAPI
from fastapi.testclient import TestClient
from passlib.hash import pbkdf2_sha256
import pytest

#######################################

#############LOCAL IMPORTS#############

from web import auth_api, device_api
from web.dependencies import services
from web.safety import HTTPSafety
from controller.types import Protocol

#######################################


class DummyDevice:
    def __init__(self, id, name):
        self.id = id
        self.name = name
        self.protocol = Protocol.NONE
        self.connected = True

    def get_device_state(self):
        return {"id": self.id, "name": self.name, "protocol": Protocol.NONE, "connected": self.connected}


class DummyDeviceManager:
    def __init__(self, devices):
        self.devices = devices


def create_app(safety, device_manager):
    app = FastAPI()
    services.set_dependencies(safety, device_manager, object(), object())
    app.include_router(auth_api.router)
    app.include_router(device_api.router)
    return app


def test_login_flow(tmp_path):
    config = {"username": "user", "password_hash": pbkdf2_sha256.hash("secret"), "jwt_secret": "secretkey"}
    config_path = tmp_path / "user_config.json"
    config_path.write_text(json.dumps(config))
    HTTPSafety.USER_CONFIG_PATH = str(config_path)
    safety = HTTPSafety()
    app = create_app(safety, DummyDeviceManager([]))
    with TestClient(app) as client:
        response = client.post("/auth/login", json={"username": "user", "password": "secret"})
        assert response.status_code == 200
        assert "token" in response.cookies


def test_get_all_devices_state(monkeypatch):
    safety = HTTPSafety()
    device = DummyDevice(1, "dev1")
    app = create_app(safety, DummyDeviceManager([device]))

    from util import functions

    monkeypatch.setattr(functions, "get_device_image", lambda device_id, default, directory: {"data": "", "type": "", "filename": ""})

    with TestClient(app) as client:
        response = client.get("/device/get_all_devices_state")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert data[0]["id"] == 1
        assert data[0]["name"] == "dev1"
