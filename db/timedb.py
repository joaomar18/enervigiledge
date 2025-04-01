from influxdb import InfluxDBClient
class TimeDBClient:

    def __init__(self):
        self.client = InfluxDBClient()

    def close(self):
        self.client.close()