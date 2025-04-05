###########EXERTNAL IMPORTS############

import asyncio
from influxdb import InfluxDBClient
from typing import List, Dict, Any
from datetime import datetime
import json

#######################################

#############LOCAL IMPORTS#############

from util.debug import LoggerManager
import util.functions as functions

#######################################


class Measurement:
    def __init__(self, db: str, data: List[Dict]):
        self.db = db
        self.data = data


class TimeDBClient:

    @staticmethod
    def to_db_format(data: List[Dict[str, Any]]) -> List[Dict[str, Any]] | None:
        formatted = []
        for item in data:
                name: str = item["name"]
                start_time: datetime = item["start_time"]
                end_time: datetime = item["end_time"]
                formatted_start = start_time.strftime("%Y-%m-%d %H:%M")
                formatted_end = end_time.strftime("%Y-%m-%d %H:%M")
                fields = {
                    k: v for k, v in item.items()
                    if k not in ("name", "start_time", "end_time") and v is not None
                }
                tags = {
                    "start_time": formatted_start,
                    "end_time": formatted_end
                }
                formatted.append({
                    "measurement": name,
                    "fields": fields,
                    "tags": tags,
                    "time": end_time
                })
        return formatted

    def __init__(self, host: str = "localhost", port: int = 8086, username: str = "root", password: str = "root"):
        self.client = InfluxDBClient(host=host, port=port, username=username, password=password)
        self.write_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        asyncio.get_event_loop().create_task(self.write_task())

    async def write_task(self):
        while True:
            if self.client != None:
                try:
                    measurement: Measurement = await self.write_queue.get()
                    await self.write_data(measurement)
                except Exception as e:
                    LoggerManager.get_logger(__name__).exception(f"Write Task: {e}")
            await asyncio.sleep(0)

    async def write_data(self, measurement: Measurement) -> bool:
        try:
            if {"name": measurement.db} not in self.client.get_list_database():
                self.client.create_database(measurement.db)

            db_data = TimeDBClient.to_db_format(measurement.data)
            if db_data is not None:
                self.client.write_points(points=db_data, database=measurement.db)
        except Exception as e:
            LoggerManager.get_logger(__name__).exception(f"Failed to write data: {e}")
            return False

    def check_db_exists(self, db: str) -> bool:
        return {"name": db} in self.client.get_list_database()

    def get_measurements_list(self, db: str) -> List[Dict]:
        self.client.switch_database(database=db)
        return self.client.get_list_measurements()

    def get_series_list(self, db: str) -> List[Dict]:
        self.client.switch_database(database=db)

    def get_measurement_data_between(
        self, device_name: str, device_id: int, measurement: str, start_time: datetime = None, end_time: datetime = None
    ) -> List[Dict] | None:
        db_name = f"{device_name}_{device_id}"
        if not self.check_db_exists(db_name):
            return []
        self.client.switch_database(db_name)
        if start_time and end_time:
            start_str = start_time.isoformat() + "Z"
            end_str = end_time.isoformat() + "Z"
            query = f"SELECT * FROM \"{measurement}\" WHERE time >= '{start_str}' AND time <= '{end_str}'"
        else:
            query = f"SELECT * FROM \"{measurement}\""
        result = self.client.query(query)
        raw_data = list(result.get_points())
        for entry in raw_data:
            entry.pop("time", None)
        return raw_data
    
    def delete_measurement_data(self, device_name: str, device_id: int, measurement: str) -> bool:
        db_name = f"{device_name}_{device_id}"
        if not self.check_db_exists(db_name):
            return False
        try:
            self.client.switch_database(db_name)
            query = f'DELETE FROM "{measurement}"'
            self.client.query(query)
            return True
        except Exception as e:
            LoggerManager.get_logger(__name__).exception(f"Failed to delete data from measurement '{measurement}' in DB '{db_name}': {e}")
            return False

    def close(self):
        self.client.close()