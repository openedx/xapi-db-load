"""
A backend that simply logs the statements to a xapi_tracking logger.

Vector just reads the log statements, so all we need to do is emit them.
All other tasks use the raw Clickhouse inserts.
"""

import logging
import sys
from logging import Logger, getLogger
from typing import List

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


class AsyncVectorTasks(BaseBackendTasks):
    def __repr__(self) -> str:
        return f"AsyncVectorTasks: {self.config['lrs_url']} -> {self.config['db_host']}"

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
            InsertXAPIEventsVector(self.config, self.logger, self.event_generator),
        ]


class InsertXAPIEventsVector(InsertXAPIEvents):
    """
    Wraps the ClickHouse direct backend so that the rest of the metadata can be sent while using
    Ralph to do the xAPI the insertion.
    """

    def __init__(self, config: dict, logger: Logger, event_generator: EventGenerator):
        super().__init__(config, logger, event_generator)

        stream_handler = logging.StreamHandler(sys.stdout)
        # This formatter is different from what the LMS uses, but is the smallest possible
        # format that passes Vector's regex
        formatter = logging.Formatter(" [{name}] [] {message}", style="{")
        stream_handler.setFormatter(formatter)
        self.xapi_logger = getLogger("xapi_tracking")
        self.xapi_logger.setLevel(logging.INFO)
        self.xapi_logger.addHandler(stream_handler)

    def _format_row(self, row: dict):
        """
        This overrides the ClickHouse backend's method to format the row for Ralph.
        """
        return row["event"]

    async def _do_insert(self, out_data: List):
        """
        POST a batch of rows to Ralph instead of inserting directly to ClickHouse.
        """
        for event_json in out_data:
            self.xapi_logger.info(event_json)
