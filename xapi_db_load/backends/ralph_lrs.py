import json
import requests
import datetime

import clickhouse_connect
from pymongo import MongoClient

from .clickhouse_lake import XAPILakeClickhouse
from .mongo_lake import XAPILakeMongo


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()


class XAPILRSRalphClickhouse(XAPILakeClickhouse):
    def __init__(self, db_host, lrs_url, lrs_username,
                 lrs_password, db_port=18123, db_username="default", db_password=None,
                 db_name="xapi"):
        self.db_host = db_host
        self.db_port = db_port
        self.db_username = db_username
        self.db_name = db_name
        self.db_password = db_password
        self.lrs_url = lrs_url
        self.lrs_username = lrs_username
        self.lrs_password = lrs_password
        self.event_raw_table_name = "xapi_events_all"
        self.event_table_name = "xapi_events_all_parsed"
        self.event_table_name_mv = "xapi_events_all_parsed_mv"

        client_options = {
            "date_time_input_format": "best_effort",  # Allows RFC dates
            "allow_experimental_object_type": 1,  # Allows JSON data type
        }

        self.client = clickhouse_connect.get_client(
            host=db_host,
            username=db_username,
            password=db_password,
            port=db_port,
            database=db_name,
            settings=client_options,
        )

    def batch_insert(self, events):
        # Ralph wants one json object per line, not an array of objects
        out_data = [json.loads(x["event"]) for x in events]
        resp = requests.post(self.lrs_url, auth=(self.lrs_username, self.lrs_password),
                             json=out_data, headers={"Content-Type": "application/json"}
                             )
        resp.raise_for_status()


class XAPILRSRalphMongo(XAPILakeMongo):
    # def __init__(self, host, port, username, password=None, database=None):
    def __init__(self, db_host, lrs_url, lrs_username,
                 lrs_password, db_port=18123, db_username="default", db_password=None,
                 db_name="statements"):
        self.db_host = db_host
        self.db_port = db_port
        self.db_username = db_username
        self.db_name = db_name
        self.db_password = db_password
        self.lrs_url = lrs_url
        self.lrs_username = lrs_username
        self.lrs_password = lrs_password

        self.event_collection_name = "foo"  # This is what Ralph calls it?

        # Provide the mongodb url to connect python to mongodb using pymongo
        # connection_string = f"mongodb://{username}:{password}@{host}/{database}"
        # For Tutor we're not using username / password
        connection_string = f"mongodb://{db_host}/{db_name}"
        print(connection_string)

        # Create a connection using MongoClient. You can import MongoClient or use
        # pymongo.MongoClient
        self.client = MongoClient(connection_string)

    def batch_insert(self, events):
        # Ralph wants one json object per line, not an array of objects
        out_data = [json.loads(x["event"]) for x in events]
        resp = requests.post(self.lrs_url, auth=(self.lrs_username, self.lrs_password),
                             json=out_data, headers={"Content-Type": "application/json"}
                             )
        resp.raise_for_status()
