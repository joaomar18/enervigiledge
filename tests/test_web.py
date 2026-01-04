########### EXTERNAL IMPORTS ############

import asyncio
import types
from typing import Set
import json
from fastapi import FastAPI
from fastapi.testclient import TestClient
from argon2 import PasswordHasher
from controller.meter.device import EnergyMeter
from model.controller.general import Protocol
from model.controller.device import EnergyMeterRecord, EnergyMeterType, EnergyMeterOptions, BaseCommunicationOptions
from controller.node.node import Node

#########################################

############# LOCAL IMPORTS #############

from web.api import device
from web.api import auth
from web.dependencies import services
from web.safety import HTTPSafety
from db.db import SQLiteDBClient
from db.timedb import TimeDBClient

#########################################


class DummyMeter(EnergyMeter):
    def __init__(self, name: str, nodes: Set[Node]):
        self.name = name
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
    HTTPSafety.USER_CONFIG_PATH = str(config_path)

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
    HTTPSafety.USER_CONFIG_PATH = str(config_path)

    safety = HTTPSafety()
    dev = DummyMeter("dev1", set())
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
        resp = client.get("/device/get_all_devices_status", headers=headers)
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert data and data[0]["id"] == 1
        assert data[0]["name"] == "dev1"
