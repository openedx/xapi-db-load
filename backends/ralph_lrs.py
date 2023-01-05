import json
import requests
import datetime

import clickhouse_connect
from backends.clickhouse_lake import XAPILakeClickhouse
from backends.mongo_lake import XAPILakeMongo
from pymongo import MongoClient


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()


class XAPILRSRalph(XAPILakeClickhouse):
    def __init__(self, host, port, username, password=None, database=None):
        self.host = host
        self.port = port
        self.username = username
        self.database = database
        self.password = password
        # FIXME: Needs http/https support
        self.xapi_endpoint = f"http://{host}:{port}/xAPI/statements/"

        # FIXME: Add support for separate db connection info
        self.event_table_name = "xapi_events_all"
        self.event_buffer_table_name = "xapi_events_buffered_all"

        self.client = clickhouse_connect.get_client(
            host="localhost",
            username="ch_admin",
            password="secret",
            port=18123,  # 19000?
            database="xapi",
            date_time_input_format="best_effort",  # Allows RFC dates
            old_parts_lifetime=10,  # Seconds, reduces disk usage
        )

    def batch_insert(self, events):
        # Ralph wants one json object per line, not an array of objects
        out_data = [json.loads(x["event"]) for x in events]
        resp = requests.post(self.xapi_endpoint, auth=(self.username, self.password),
                             json=out_data, headers={"Content-Type": "application/json"})
        resp.raise_for_status()


class XAPILRSRalphMongo(XAPILakeMongo):
    def __init__(self, host, port, username, password=None, database=None):
        self.host = host
        self.port = port
        self.username = username
        self.database = database
        self.password = password
        # FIXME: Needs http/https support
        self.xapi_endpoint = f"http://{host}:{port}/xAPI/statements/"

        # FIXME: Needs separate db / LRS support instead of hard coding
        self.event_collection_name = "foo"  # This is what Ralph calls it?

        self.mongo_db = "statements"

        # Provide the mongodb url to connect python to mongodb using pymongo
        # connection_string = f"mongodb://{username}:{password}@{host}/{database}"
        # For Tutor we're not using username / password
        connection_string = f"mongodb://localhost/statements"
        print(connection_string)

        # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
        self.client = MongoClient(connection_string)

    def batch_insert(self, events):
        # Ralph wants one json object per line, not an array of objects
        out_data = [json.loads(x["event"]) for x in events]
        resp = requests.post(self.xapi_endpoint, auth=(self.username, self.password),
                             json=out_data, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
