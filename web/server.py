import asyncio
import json
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from uvicorn import Config, Server
import multipart
import util.debug as debug

from controller.device import Device, DeviceManager
from db.timedb import TimeDBClient


class HTTPServer:
    def __init__(self, host: str, port: int, device_manager: DeviceManager, timedb: TimeDBClient):
        self.host = host
        self.port = port
        self.device_manager = device_manager
        self.timedb = timedb
        self.server = FastAPI()
        self.setup_routes()
        asyncio.get_event_loop().create_task(self.run_server())

    def setup_routes(self):
        @self.server.post("/get_logs")
        async def get_logs_from_measurement(request: Request):
            try:
                request_data: dict = await request.json()
                name = request_data.get("name")
                id = request_data.get("id")
                measurement = request_data.get("measurement")
                device = self.device_manager.get_device(name, id)
                if not device:
                    raise KeyError(f"Device with name {name} and id {id} does not exist.")
                if not any(measurement == node.name for node in device.nodes):
                    raise KeyError(f"Node with name {measurement} does not exist in device {name} with id {id}")
                response = self.timedb.get_measurement_data_between(device_name=name, device_id=id, measurement=measurement)
            except Exception as e:
                if measurement:
                    debug.logger.error(f"Failed to retrieve data for measurement '{measurement}' in time db: {e}")
                else:
                    debug.logger.error(f"Failed to retrieve data in time db: {e}")
                return JSONResponse(content={"error": str(e)})
            return JSONResponse(content=response)

        @self.server.post("/delete_logs")
        async def delete_logs_from_measurement(request: Request):
            try:
                request_data: dict = await request.json()
                name = request_data.get("name")
                id = request_data.get("id")
                measurement = request_data.get("measurement")
                device = self.device_manager.get_device(name, id)
                if not device:
                    raise KeyError(f"Device with name {name} and id {id} does not exist.")
                if not any(measurement == node.name for node in device.nodes):
                    raise KeyError(f"Node with name {measurement} does not exist in device {name} with id {id}")
                result = self.timedb.delete_measurement_data(device_name=name, device_id=id, measurement=measurement)
                response = {}
                if result:
                    response["result"] = f"Sucessfully deleted {measurement} node logs from device {name} with id {id}"
                else:
                    response["result"] = f"Could not delete {measurement} node logs from device {name} with id {id}"
            except Exception as e:
                if measurement:
                    debug.logger.error(f"Failed to delete data for measurement '{measurement}' in time db: {e}")
                else:
                    debug.logger.error(f"Failed to delete data in time db: {e}")
                return JSONResponse(content={"error": str(e)})
            return JSONResponse(content=response)

    async def run_server(self):
        config = Config(app=self.server, host=self.host, port=self.port, reload=False)
        server = Server(config)
        await server.serve()
