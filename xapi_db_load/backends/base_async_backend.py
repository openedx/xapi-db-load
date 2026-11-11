"""
Base asynchronous backend, abstract classes that other backends inherit from.
"""

import asyncio
from abc import abstractmethod
from logging import Logger
from typing import (
    Dict,
    List,
    Type,
)

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import (
    DatabaseError,
    OperationalError,
)

from xapi_db_load.generate_load_async import (
    EventGenerator,
    Waiter,
)

# How many characters of an SQL statement go into an error log line. The
# statements carry whole batches of rows and can be many MB.
MAX_LOGGED_SQL_LENGTH = 2000

# How many leading characters of the S3 access key id are left readable in logs,
# enough to tell which key was used. The secret is always hidden whole.
VISIBLE_S3_KEY_CHARS = 4


def redact_credentials(text: str, s3_key: str | None, s3_secret: str | None) -> str:
    """
    Return text with the S3 secret hidden and all but the start of the S3 key
    hidden, for SQL and error messages that embed them before they are logged
    or raised.
    """
    if s3_secret:
        text = text.replace(s3_secret, "****")
    if s3_key:
        visible = s3_key[:VISIBLE_S3_KEY_CHARS] if len(s3_key) > VISIBLE_S3_KEY_CHARS else ""
        text = text.replace(s3_key, f"{visible}****")
    return text


class BaseBackendTasks:
    """
    Base class for backend task management classes.

    Defines the basic interface and convenience methods.
    """

    def __init__(
        self,
        config: dict,
        logger: Logger,
        event_generator: EventGenerator,
    ):
        self.config = config
        self.logger = logger
        self.event_generator = event_generator

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}: {self.config.get('db_host', 'No ClickHouse configured')}"

    @abstractmethod
    def get_test_data_tasks(self) -> List[Waiter]:
        """Return the tasks to be run to generate test data."""

    def competency_tasks(self, *task_classes: Type[Waiter], **kwargs) -> List[Waiter]:
        """
        Return the learner competency status tasks, only for a competency load.

        A "tags" load has no statuses to write, and an install without the
        tutor-contrib-aspects competency migrations has no tables to load them
        into, so it must not touch those tables at all.
        """
        if not self.event_generator.is_competency_load:
            return []
        return [
            task_class(self.config, self.logger, self.event_generator, **kwargs)
            for task_class in task_classes
        ]

    def get_backend_summary(self) -> Dict:
        """
        Get a dict describing our backend configuration.
        """
        return {
            "backend": str(self),
            "num_xapi_batches": self.config["num_xapi_batches"],
            "batch_size": self.config["batch_size"],
            "total_events": self.config["num_xapi_batches"] * self.config["batch_size"],
            "num_actors": self.config["num_actors"],
            "num_actor_profile_changes": self.config["num_actor_profile_changes"],
            "num_courses": sum(
                self.config["num_course_sizes"][i]
                for i in self.config["num_course_sizes"]
            ),
            "num_course_publishes": self.config["num_course_publishes"],
        }


class BaseClickhouseBackend(Waiter):  # pylint: disable=abstract-method
    """
    Abstract implementation for ClickHouse backends.

    Handles ClickHouse client configuration and connection management.
    Intentionally partial implementation: _run_task/_run_db_load_task are left to subclasses.
    """

    client: AsyncClient | None = None
    host: str
    port: int
    username: str
    database: str
    db_password: str
    s3_key: str | None
    s3_secret: str | None
    client: AsyncClient | None = None
    batch_size: int

    def __init__(self, config: Dict, logger: Logger, event_generator: EventGenerator):
        super().__init__(config, logger, event_generator)

        self.host = config.get("db_host", "localhost")
        self.port = config.get("db_port", 18123)
        self.username = config.get("db_username", "default")
        self.database = config.get("db_name", "xapi")
        self.db_password = config.get("db_password", "")
        self.s3_key = config.get("s3_key")
        self.s3_secret = config.get("s3_secret")
        self.batch_size = self.config["batch_size"]

    def redact(self, text: str) -> str:
        """
        Return text with the configured S3 credentials redacted, see
        ``redact_credentials``.
        """
        return redact_credentials(text, self.s3_key, self.s3_secret)

    def sql_for_log(self, sql: str) -> str:
        """
        Return an SQL statement fit for a log line: credentials redacted and
        truncated to ``MAX_LOGGED_SQL_LENGTH`` characters.
        """
        sql = self.redact(sql)
        if len(sql) > MAX_LOGGED_SQL_LENGTH:
            sql = f"{sql[:MAX_LOGGED_SQL_LENGTH]}... ({len(sql)} characters in all)"
        return sql

    async def set_client(self):
        """
        Set up the ClickHouse client and connect.
        """
        client_options = {
            "date_time_input_format": "best_effort",  # Allows RFC dates
            "receive_timeout": 300,
        }

        # For some reason get_client isn't automatically setting secure based on the port
        # so we have to do it ourselves. This is obviously limiting, but should be 90% correct
        # and keeps us from adding yet another command line option.
        secure = str(self.port).endswith("443") or str(self.port).endswith("440")

        self.client = await clickhouse_connect.get_async_client(
            host=self.host,
            username=self.username,
            password=self.db_password,
            port=self.port,
            database=self.database,
            settings=client_options,
            secure=secure,
        )

    async def _insert_list_sql_retry(
        self, data_list: List, table: str, database: str = "event_sink"
    ):
        """
        Wrap up inserts that join values to reduce some boilerplate.
        """
        sql = f"""
                INSERT INTO {database}.{table}
                VALUES {",".join(data_list)}
        """

        await self._run_sql_command(sql)

    async def _run_sql_command(self, sql: str):
        """
        Run an SQL command, connecting first if needed. No retry: on failure it
        logs and re-raises with the S3 credentials redacted.
        """
        try:
            if not self.client:
                await self.set_client()
            assert self.client
            await self.client.command(sql)
        except OperationalError as e:
            message = self.redact(str(e))
            self.logger.error("ClickHouse OperationalError (connection problem):")
            self.logger.error(message)
            # Raised anew, without the original as its cause: the original's
            # message can quote the SQL, credentials included. Same class and
            # error code, so callers can still tell the errors apart.
            raise type(e)(message, code=e.code, name=e.name) from None
        except DatabaseError as e:
            message = self.redact(str(e))
            self.logger.error("ClickHouse DatabaseError:")
            self.logger.error(message)
            self.logger.error(self.sql_for_log(sql))
            raise type(e)(message, code=e.code, name=e.name) from None


class QueueBackend(BaseClickhouseBackend):
    """
    Queue-based backend for tasks dealing with lots of data in batches.
    """

    def __init__(self, config: Dict, logger: Logger, event_generator: EventGenerator):
        super().__init__(config, logger, event_generator)
        # Limiting the queue size prevents us from creating a huge number of tasks that just
        # sit in memory for a long time. _batch_worker will block on inserting new tasks to the
        # queue until there is more room.
        self.queue = asyncio.Queue(maxsize=20)

    @abstractmethod
    async def _populate_queue(self):
        """Subclasses override this to enqueue work for their particular data task."""

    @abstractmethod
    async def _process_queue_item(
        self, worker_id: int, batch_id: int, batch: List | int
    ):
        """Subclasses override this to process a single batch item from the queue."""

    async def _batch_worker(self, worker_id):
        """
        This function is a worker thread that runs asynchronously.
        """
        self.logger.debug(f"   {self.task_name} worker {worker_id} up")

        # This keeps track of which batch we're on, some tasks need it to partition data
        # in a deterministic way.
        batch_id = 0

        while True:
            self.logger.debug(
                f"   {self.task_name} worker {worker_id} waiting for task"
            )
            batch = await self.queue.get()
            batch_id += 1

            self.logger.debug(
                f"   {self.task_name} worker {worker_id} starting batch {batch_id}"
            )

            try:
                await self._process_queue_item(worker_id, batch_id, batch)
            except Exception as e:  # pylint: disable=broad-exception-caught
                self.logger.error(
                    f"   {self.task_name} worker {worker_id} found an error in batch {batch_id}.",
                    exc_info=e,
                )

            self.queue.task_done()

    async def _run_task(self):
        """
        Feed the queue to keep the batches running.
        """
        # Create worker tasks to process the queue
        tasks = []

        # Create the worker tasks, we track them so that we can cancel them when we're done
        # adding batch ids and the queue is empty.
        tasks = []
        for i in range(self.config["num_workers"]):
            self.logger.info(f"Starting {self.task_name} worker {i}")
            tasks.append(asyncio.create_task(self._batch_worker(i)))

        # Subclasses override this to populate the queue with their data. The contents of the
        # queue can be very different so we don't try to define a common interface.
        await self._populate_queue()

        # The queue has been populated, now we wait for it to empty
        while self.queue.qsize() > 0:
            await asyncio.sleep(5)
            self.logger.info(
                f"Waiting for {self.task_name} queue to empty. Size is: {self.queue.qsize()}"
            )

        # Block until the last batch is marked as complete
        await self.queue.join()

        # This breaks the worker threads out of their "while True" loops.
        self.logger.info(f"{self.task_name} cancelling workers")
        for t in tasks:
            t.cancel()

        # Wait for all of the worker threads to return from the cancel()
        # TODO: We should do some bookkeeping here to report how many batches each worker
        # completed, and save off exceptions to report. This call actually returns a list of
        # return codes from the workers which we currently ignore since they're all None.
        await asyncio.gather(*tasks, return_exceptions=True)
        self.logger.info(f" {self.task_name} all workers done")

        # Force completion to 100%, we're done.
        self.complete_pct = 1.0
