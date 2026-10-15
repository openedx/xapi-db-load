"""
Load a generated journeys dataset into an Aspects install.

Event sink rows and the expected results are inserted straight into ClickHouse. The xAPI events
go through the configured ``backend``, so a test can cover the same path real events take:

- ``clickhouse``: inserted straight into ``xapi_events_all``.
- ``ralph``: POSTed to the LRS, without the duplicate events, which an LRS rejects.
- ``vector``: logged to stdout in the format Vector reads from the LMS logs.

The expected results go into ``expected_db_name`` (default ``journeys``) as
``expected_engagement`` and ``expected_video_seconds``, with ``event_ids`` listing every event in
the dataset. Ralph and Vector insert asynchronously, so ``wait`` polls until every event id is in
``xapi_events_all``.
"""

import csv
import gzip
import json
import logging
import os
import sys
import time
from typing import (
    Any,
    Dict,
    Iterator,
    List,
)

import clickhouse_connect
import requests

from xapi_db_load.journeys import oracle

# Dataset file -> event sink table. Each file has the table's columns, in order.
SINK_FILES = {
    "courses": "course_overviews",
    "blocks": "course_blocks",
    "external_ids": "external_id",
    "user_profiles": "user_profile",
}
XAPI_COLUMNS = ["event_id", "emission_time", "event"]
BACKENDS = ("clickhouse", "ralph", "vector")
EXPECTED_TABLES = {
    "expected_engagement": (
        oracle.ENGAGEMENT_COLUMNS,
        {"done": "UInt32", "total": "UInt32", "done_truth": "UInt32"},
    ),
    "expected_video_seconds": (
        oracle.VIDEO_SECONDS_COLUMNS,
        {c: "UInt32" for c in oracle.VIDEO_SECONDS_COLUMNS if c.endswith(("seconds", "duration", "views"))},
    ),
}
RALPH_BATCH_SIZE = 500


def _read_gz(path: str) -> bytes:
    with gzip.open(path, "rb") as f:
        return f.read()


def _xapi_rows(path: str) -> Iterator[List[str]]:
    with gzip.open(path, "rt", newline="") as f:
        yield from csv.reader(f)


class JourneyLoader:
    """Loads one dataset directory, as written by the ``journeys`` command."""

    def __init__(self, config: Dict, dataset_dir: str, logger: logging.Logger, client: Any = None):
        self.config = config
        self.dataset_dir = dataset_dir
        self.log = logger
        self.backend = config.get("backend", "clickhouse")
        if self.backend not in BACKENDS:
            raise ValueError(f"backend must be one of {', '.join(BACKENDS)}, got {self.backend!r}.")
        try:
            with open(self._path("manifest.json")) as f:
                self.manifest = json.load(f)
        except FileNotFoundError as e:
            raise ValueError(
                f"{dataset_dir} has no manifest.json; generate it with the journeys command first."
            ) from e
        self.xapi_db = config.get("db_name", "xapi")
        self.sink_db = config.get("event_sink_db_name", "event_sink")
        self.expected_db = config.get("expected_db_name", "journeys")
        self.client = client or self._connect()

    def _connect(self):
        port = self.config.get("db_port", 18123)
        return clickhouse_connect.get_client(
            host=self.config.get("db_host", "localhost"),
            port=port,
            username=self.config.get("db_username", "default"),
            password=self.config.get("db_password", ""),
            secure=str(port).endswith(("443", "440")),
            settings={"date_time_input_format": "best_effort"},
        )

    def _path(self, name: str) -> str:
        return os.path.join(self.dataset_dir, name)

    def run(self, wait_seconds: float = 0) -> str:
        """Load everything and return a one-line summary."""
        self.load_event_sink()
        n_events = self.load_expected()
        self.load_xapi()
        self.wait(n_events, wait_seconds)
        return (
            f"Loaded {n_events} xAPI events for {self.manifest['num_enrollments']} enrollments "
            f"in {self.manifest['num_course_runs']} course runs via {self.backend}; expected "
            f"results are in {self.expected_db}."
        )

    def load_event_sink(self) -> None:
        """Insert the course, block, user and profile rows."""
        for name, table in SINK_FILES.items():
            self.log.info(f"Inserting {name} into {self.sink_db}.{table}")
            self.client.raw_insert(
                f"{self.sink_db}.{table}", insert_block=_read_gz(self._path(f"{name}.csv.gz")),
                fmt="CSV",
            )

    def load_expected(self) -> int:
        """Replace the expected-results tables; return the number of distinct events."""
        self.client.command(f"create database if not exists {self.expected_db}")
        for table, (columns, types) in EXPECTED_TABLES.items():
            schema = ", ".join(f"{c} {types.get(c, 'String')}" for c in columns)
            self.client.command(f"drop table if exists {self.expected_db}.{table}")
            self.client.command(
                f"create table {self.expected_db}.{table} ({schema}) "
                f"engine = MergeTree order by tuple()"
            )
            self.client.raw_insert(
                f"{self.expected_db}.{table}", insert_block=_read_gz(self._path(f"{table}.csv.gz")),
                fmt="CSVWithNames",
            )
        ids = sorted({row[0] for row in _xapi_rows(self._path("xapi.csv.gz"))})
        self.client.command(f"drop table if exists {self.expected_db}.event_ids")
        self.client.command(
            f"create table {self.expected_db}.event_ids (event_id UUID) "
            f"engine = MergeTree order by event_id"
        )
        self.client.raw_insert(
            f"{self.expected_db}.event_ids", insert_block="\n".join(ids).encode(), fmt="CSV"
        )
        return len(ids)

    def load_xapi(self) -> None:
        """Send the xAPI events through the configured backend."""
        path = self._path("xapi.csv.gz")
        self.log.info(f"Sending xAPI events via {self.backend}")
        if self.backend == "clickhouse":
            self.client.raw_insert(
                f"{self.xapi_db}.xapi_events_all", column_names=XAPI_COLUMNS,
                insert_block=_read_gz(path), fmt="CSV",
            )
        elif self.backend == "ralph":
            self._post_to_ralph(path)
        else:
            self._log_for_vector(path)

    def _post_to_ralph(self, path: str) -> None:
        """POST the events to the LRS in batches, skipping repeated event ids."""
        seen = set()
        batch: List[Dict] = []
        for event_id, _, event in _xapi_rows(path):
            if event_id in seen:
                continue
            seen.add(event_id)
            batch.append(json.loads(event))
            if len(batch) == RALPH_BATCH_SIZE:
                self._post_batch(batch)
                batch = []
        if batch:
            self._post_batch(batch)

    def _post_batch(self, batch: List[Dict]) -> None:
        """POST one batch of statements, failing on any error response."""
        resp = requests.post(
            self.config["lrs_url"],
            auth=(self.config["lrs_username"], self.config["lrs_password"]),
            json=batch,
            timeout=self.config.get("lrs_request_timeout", 120),
        )
        resp.raise_for_status()

    def _log_for_vector(self, path: str) -> None:
        # The smallest line format that passes Vector's LMS tracking log regex.
        for _, _, event in _xapi_rows(path):
            sys.stdout.write(f" [xapi_tracking] [] {event}\n")
        sys.stdout.flush()

    def wait(self, n_events: int, wait_seconds: float, poll_seconds: float = 5) -> None:
        """Wait until every event in the dataset is in xapi_events_all."""
        deadline = time.monotonic() + wait_seconds
        while True:
            arrived = self.client.command(
                f"select count() from {self.expected_db}.event_ids where event_id in "
                f"(select event_id from {self.xapi_db}.xapi_events_all)"
            )
            if int(arrived) >= n_events:
                self.log.info(f"All {n_events} xAPI events are in {self.xapi_db}.xapi_events_all")
                return
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Only {arrived} of {n_events} xAPI events reached "
                    f"{self.xapi_db}.xapi_events_all within {wait_seconds} s."
                )
            self.log.info(f"{arrived} of {n_events} xAPI events have arrived, waiting")
            time.sleep(poll_seconds)
