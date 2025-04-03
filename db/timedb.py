###########EXERTNAL IMPORTS############

import asyncio
from influxdb import InfluxDBClient
from typing import List, Dict, Any
from datetime import datetime

#######################################

#############LOCAL IMPORTS#############

import util.debug as debug
import util.functions as functions

#######################################


class Measurement:
    def __init__(self, db: str, data: List[Dict]):
        self.db = db
        self.data = data


class TimeDBClient:

    @staticmethod
    def to_db_format(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        formatted = []
        for item in data:
            fields = {k: v for k, v in item.items() if k not in ("name", "start_time", "end_time") and v is not None}
            tags = {"start_time": functions.datetime_to_tag(item["start_time"]), "end_time": functions.datetime_to_tag(item["end_time"])}
            timestamp: datetime = item.get("end_time")

            formatted.append({"measurement": item["name"], "fields": fields, "tags": tags, "time": timestamp.isoformat() if timestamp else None})
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
                    print(f"Sucessfully writed measurement to {measurement.db}")
                except Exception as e:
                    debug.logger.error(f"Time DB Client - Write Task: {e}")
            await asyncio.sleep(0)

    async def write_data(self, measurement: Measurement) -> bool:
        try:
            if {"name": measurement.db} not in self.client.get_list_database():
                self.client.create_database(measurement.db)

            db_data = TimeDBClient.to_db_format(measurement.data)
            print(db_data)
            self.client.write_points(points=db_data, database=measurement.db)
        except Exception as e:
            debug.logger.error(f"Failed to write data to time db: {e}")
            return False

    def check_db_exists(self, db: str) -> bool:
        return {"name": db} in self.client.get_list_database()

    def get_measurements_list(self, db: str) -> List[Dict]:
        self.client.switch_database(database=db)
        return self.client.get_list_measurements()

    def get_series_list(self, db: str) -> List[Dict]:
        self.client.switch_database(database=db)

    def get_data(self, query_str: str, db: str) -> List[Dict]:
        try:
            result = self.client.query(query=query_str, database=db)
            return {result.get_points()}
        except Exception as e:
            debug.logger.error(f"Failed to execute query in time db: {e}")
            return None

    def close(self):
        self.client.close()
