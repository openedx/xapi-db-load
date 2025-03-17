"""
Base asynchronous backend, abstract classes that other backends inherit from.
"""

import asyncio
from logging import Logger
from typing import Dict, List

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

from xapi_db_load.generate_load_async import EventGenerator, Waiter


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

    def get_test_data_tasks(self) -> List[Waiter]:
        """
        Return the tasks to be run to generate test data.
        """
        raise NotImplementedError("get_test_data_tasks not implemented")

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


class BaseClickhouseBackend(Waiter):
    """
    Abstract implementation for ClickHouse backends.

    Handles ClickHouse client configuration and connection management.
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

        await self._insert_sql_with_retry(sql)

    async def _insert_sql_with_retry(self, sql: str):
        """
        Wrap insert commands with a single retry.
        """
        try:
            # from clickhouse_connect.driver.asyncclient import AsyncClient
            # Sometimes the connection randomly dies, this gives us a second shot in that case
            if not self.client:
                await self.set_client()
            assert self.client
            await self.client.command(sql)
        except OperationalError as e:
            self.logger.error(e)
            self.logger.error("ClickHouse OperationalError, trying to reconnect.")
            raise
            # await self.set_client()
            # self.logger.error("Retrying insert...")
            # await self.client.command(sql)
        except DatabaseError as e:
            self.logger.error("ClickHouse DatabaseError:")
            self.logger.error(e)
            self.logger.error(sql)
            raise


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

    async def _populate_queue(self):
        """
        Subclasses override this to enque work for their particular data task.
        """
        raise NotImplementedError(
            f"_get_batch_data not implemented for {self.task_name}"
        )

    async def _process_queue_item(
        self, worker_id: int, batch_id: int, batch: List | int
    ):
        """
        Subclasses override this to create workers that process their data for a batch.
        """
        raise NotImplementedError(
            f"_process_queue_item not implemented for {self.task_name}"
        )

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
            except Exception as e:
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
