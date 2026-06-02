"""
A backend that uses Ralph to insert the xAPI events directly into ClickHouse.

This simply overrides the method that sends initial enrollment and random xAPI events to
send via Ralph POST instead of ClickHouse insert. All other event types are handled via the
ClickHouse backend since Ralph doesn't handle those kinds of data.
"""

import json
from logging import Logger
from typing import List

import requests

from xapi_db_load.backends.base_async_backend import (
    BaseBackendTasks,
)
from xapi_db_load.backends.clickhouse import (
    InsertBlocks,
    InsertCourses,
    InsertExternalIDs,
    InsertInitialEnrollments,
    InsertObjectTags,
    InsertProfiles,
    InsertTags,
    InsertTaxonomies,
    InsertXAPIEvents,
)
from xapi_db_load.generate_load_async import EventGenerator


class AsyncRalphTasks(BaseBackendTasks):
    def __repr__(self) -> str:
        return f"AsyncRalphTasks: {self.config['lrs_url']} -> {self.config['db_host']}"

    def get_test_data_tasks(self):
        """
        Return the tasks to be run.
        """
        return [
            self.event_generator,
            InsertInitialEnrollments(self.config, self.logger, self.event_generator),
            InsertCourses(self.config, self.logger, self.event_generator),
            InsertBlocks(self.config, self.logger, self.event_generator),
            InsertObjectTags(self.config, self.logger, self.event_generator),
            InsertTaxonomies(self.config, self.logger, self.event_generator),
            InsertTags(self.config, self.logger, self.event_generator),
            InsertExternalIDs(self.config, self.logger, self.event_generator),
            InsertProfiles(self.config, self.logger, self.event_generator),
            # This is the only change from the ClickHouse backend
            InsertXAPIEventsRalph(self.config, self.logger, self.event_generator),
        ]


class InsertXAPIEventsRalph(InsertXAPIEvents):
    """
    Wraps the ClickHouse direct backend so that the rest of the metadata can be sent while using
    Ralph to do the xAPI the insertion.
    """

    # Default timeout (seconds) applied to Ralph POST requests when the config
    # does not specify one. Generous so legitimately-slow Ralph servers are not
    # cut off, but finite so a hung endpoint does not stall the whole run.
    # Set ``lrs_request_timeout: null`` in config to restore unbounded waits.
    DEFAULT_LRS_REQUEST_TIMEOUT = 120

    def __init__(self, config: dict, logger: Logger, event_generator: EventGenerator):
        super().__init__(config, logger, event_generator)

        self.lrs_url = config["lrs_url"]
        self.lrs_username = config["lrs_username"]
        self.lrs_password = config["lrs_password"]
        # ``None`` is a valid value for ``requests`` and means "wait forever".
        self.lrs_request_timeout = config.get(
            "lrs_request_timeout", self.DEFAULT_LRS_REQUEST_TIMEOUT
        )

    def _format_row(self, row: dict):
        """
        This overrides the ClickHouse backend's method to format the row for Ralph.
        """
        return json.loads(row["event"])

    async def _do_insert(self, out_data: List):
        """
        POST a batch of rows to Ralph instead of inserting directly to ClickHouse.

        A timeout is applied (configurable via ``lrs_request_timeout``) so a
        hung Ralph endpoint cannot stall the run indefinitely. ``HTTPError`` and
        connection-level failures are logged with the offending payload and
        re-raised so the worker's existing retry/error-count handling applies.
        """
        try:
            resp = requests.post(
                self.lrs_url,
                auth=(self.lrs_username, self.lrs_password),
                json=out_data,
                headers={"Content-Type": "application/json"},
                timeout=self.lrs_request_timeout,
            )
        except requests.RequestException as exc:
            # Connection / timeout / SSL / etc. log the payload
            # and re-raise so the worker loop records the failure.
            self.logger.error(
                "Ralph POST to %s failed before a response was received.",
                self.lrs_url,
                exc_info=exc,
            )
            self.logger.error(json.dumps(out_data))
            raise

        try:
            resp.raise_for_status()
        except requests.HTTPError:
            self.logger.error(json.dumps(out_data))
            raise
