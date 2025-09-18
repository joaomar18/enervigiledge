########### EXTERNAL IMPORTS ############

import json
from fastapi import FastAPI
from fastapi.testclient import TestClient
from argon2 import PasswordHasher

#########################################

############# LOCAL IMPORTS #############

from web.api import device
from web.api import auth
from web.dependencies import services
from web.safety import HTTPSafety
from model.controller.general import Protocol
from db.db import SQLiteDBClient
from db.timedb import TimeDBClient

#########################################


class DummyDevice:
    def __init__(self, id, name):
        self.id = id
        self.name = name
        self.protocol = Protocol.NONE
        self.connected = True

    def get_device(self):
        # FastAPI/jsonable_encoder can handle Enum; if not, switch to self.protocol.name
        return {
            "id": self.id,
            "name": self.name,
            "protocol": self.protocol,
            "connected": self.connected,
        }


class DummyDeviceManager:
    def __init__(self, devices):
        self.devices = devices

    def get_all_devices(self):
        return self.devices


class DummySQLiteDB(SQLiteDBClient):
    pass


class DummyTimeDB(TimeDBClient):
    def get_measurement_data_between(self, *args, **kwargs):
        return []


def create_app(safety, device_manager):
    app = FastAPI()
    # Provide the full dependency set
    services.set_dependencies(safety, device_manager, DummySQLiteDB(), DummyTimeDB())
    app.include_router(auth.router)
    app.include_router(device.router)
    return app


def test_login_flow(tmp_path):
    config = {
        "username": "user",
        "password_hash": PasswordHasher().hash("secret"),
        "jwt_secret": "secretkey",
    }
    config_path = tmp_path / "user_config.json"
    config_path.write_text(json.dumps(config))
    HTTPSafety.USER_CONFIG_PATH = str(config_path)  # type: ignore[assignment]

    safety = HTTPSafety()
    app = create_app(safety, DummyDeviceManager([]))

    with TestClient(app) as client:
        resp = client.post("/auth/login", json={"username": "user", "password": "secret"})
        assert resp.status_code == 200
        assert "token" in resp.cookies


def test_get_all_devices(monkeypatch, tmp_path):
    # Auth setup
    config = {
        "username": "user",
        "password_hash": PasswordHasher().hash("secret"),
        "jwt_secret": "secretkey",
    }
    config_path = tmp_path / "user_config.json"
    config_path.write_text(json.dumps(config))
    HTTPSafety.USER_CONFIG_PATH = str(config_path)  # type: ignore[assignment]

    safety = HTTPSafety()
    dev = DummyDevice(1, "dev1")
    app = create_app(safety, DummyDeviceManager([dev]))

    # Patch image provider used by the endpoint
    from util.functions import images

    monkeypatch.setattr(
        images,
        "get_device_image",
        lambda device_id, default, directory: {"data": "", "type": "", "filename": ""},
    )

    with TestClient(app) as client:
        # 1) Login
        resp = client.post("/auth/login", json={"username": "user", "password": "secret"})
        assert resp.status_code == 200

        # 2) Get token (prefer JSON; fallback to cookie)
        token = resp.json().get("token") or resp.cookies.get("token")
        assert token, "Login must return a token in JSON or cookie"

        headers = {"Authorization": f"Bearer {token}"}

        # 3) Call endpoints with header
        resp = client.get("/device/get_all_devices", headers=headers)
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert data and data[0]["id"] == 1
        assert data[0]["name"] == "dev1"
