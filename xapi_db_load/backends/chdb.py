"""
CHDB backend implementation.

Uses CHDB to write data partitioned, lz4 compressed, ClickHouse Native tables to S3. Then loads the
data into ClickHouse via "INSERT INTO ... TABLE FUNCTION S3(...)".

This is currently the fastest way to load large amounts of data, but slower than CSV for smaller
datasets.
"""

import asyncio
import uuid
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime
from logging import Logger
from threading import Lock
from typing import Any, Dict, List
from urllib.parse import urljoin

import boto3
import chdb

from xapi_db_load.backends.base_async_backend import (
    BaseBackendTasks,
    QueueBackend,
)
from xapi_db_load.generate_load_async import EventGenerator
from xapi_db_load.waiter import Waiter


class AsyncCHDBTasks(BaseBackendTasks):
    def __init__(
        self,
        config: dict,
        logger: Logger,
        event_generator: EventGenerator,
    ):
        super().__init__(config, logger, event_generator)

        # Once we have created our file on S3, we create a new load task on this loader to
        # actually import the data to ClickHouse.
        self.db_loader = LoadFromS3(config, logger, event_generator, None)

    def __repr__(self) -> str:
        return f"AsyncCHDBTasks: {self.config['s3_bucket']}/{self.config['s3_prefix']} -> {self.config['db_host']}"

    def get_test_data_tasks(self) -> List[Waiter]:
        """
        Return the tasks to be run.
        """
        return [
            # The event generator is a special Waiter task that does the setup necessary for
            # the other tasks to run. They will not start until this task completes.
            self.event_generator,
            WriteInitialEnrollments(
                self.config,
                self.logger,
                self.event_generator,
                self.db_loader,
            ),
            WriteCourses(
                self.config,
                self.logger,
                self.event_generator,
                self.db_loader,
            ),
            WriteBlocks(
                self.config,
                self.logger,
                self.event_generator,
                self.db_loader,
            ),
            WriteObjectTags(
                self.config,
                self.logger,
                self.event_generator,
                self.db_loader,
            ),
            WriteTaxonomies(
                self.config,
                self.logger,
                self.event_generator,
                self.db_loader,
            ),
            WriteTags(
                self.config,
                self.logger,
                self.event_generator,
                self.db_loader,
            ),
            WriteExternalIDs(
                self.config,
                self.logger,
                self.event_generator,
                self.db_loader,
            ),
            WriteUserProfiles(
                self.config,
                self.logger,
                self.event_generator,
                self.db_loader,
            ),
            WriteXAPIEvents(
                self.config,
                self.logger,
                self.event_generator,
                self.db_loader,
            ),
            # This is an instance of LoadFromS3 that continuously runs while the other
            # non-EventGenerator tasks are running. The other tasks can add to its queue of files
            # to load to ClickHouse. When the other tasks complete this task will empty its queue
            # then exit.
            self.db_loader,
        ]


class XAPILakeCHDBAsync(QueueBackend):
    """
    Abstract base class for CHDB async jobs.

    This class extends the basic ClickHouse functionality to add CHDB specifc configuration, a
    common interface, and reusable functionality for loading from S3 to ClickHouse.
    """

    task_name = "XAPI Lake CHDB Async"
    # The column definition of the table that CHDB will create for the inheriting class.
    table_definition = None
    # CHDB will partition the table by this key, if set
    partition_key = None
    # The name of the ClickHouse schema to load this table to
    schema = None
    # The name of the ClickHouse table to load this data to
    table = None
    # The S3 prefix to load from. This is a wildcard that will be used to match the files in S3
    # that were created by the CHDB job for a specific load job, and will find all of the
    # partitioned files of that type.
    s3_load_prefix = None

    def __init__(
        self,
        config: Dict,
        logger: Logger,
        event_generator: EventGenerator,
        db_loader: "LoadFromS3|None",
    ):
        super().__init__(config, logger, event_generator)
        self.db_loader = db_loader
        self.s3_bucket = self.config["s3_bucket"]
        self.s3_prefix = self.config["s3_prefix"]
        self.s3_source_location = urljoin(
            f"https://{self.s3_bucket}.s3.amazonaws.com/", self.s3_prefix
        )

        # This prevents the LoadFromS3 task from registering itself as a task
        if self.db_loader:
            self.db_loader.register_task(self)

    async def _run_db_load_task(self):
        """
        Find all files in S3 that match the s3_load_prefix and add them to the LoadFromS3 queue.

        This task is only run when `--load_db_only` is passed to the `load-db` command.
        """
        if not self.s3_load_prefix:
            self.logger.info(f"No S3 wildcard for {self.task_name}, skipping load.")
            return

        if not self.db_loader:
            raise RuntimeError("LoadFromS3 not configured, cannot load from S3.")

        if not self.schema or not self.table:
            raise RuntimeError("Schema and table must be set to load from S3.")

        session = boto3.Session(
            aws_access_key_id=self.config["s3_key"],
            aws_secret_access_key=self.config["s3_secret"],
        )
        start_after = ""
        s3_paginator = session.client("s3").get_paginator("list_objects_v2")

        # Get the total number of files that match our prefix in S3 first, this is slow but probably
        # not as slow as building up a huge list in memory then looping through that.
        for page in s3_paginator.paginate(
            Bucket=self.s3_bucket,
            Prefix=urljoin(self.s3_prefix, self.s3_load_prefix),
            StartAfter=start_after,
        ):
            for content in page.get("Contents", ()):
                # Keep track of how many tasks we will need to run
                self.update_total_task_count(increment_by=1)

        # Now actually loop through the files using a paginator, otherwise S3 may not return all of
        # the files.
        curr = 0
        start_after = ""
        for page in s3_paginator.paginate(
            Bucket=self.s3_bucket,
            Prefix=urljoin(self.s3_prefix, self.s3_load_prefix),
            StartAfter=start_after,
        ):
            for content in page.get("Contents", ()):
                curr += 1
                k = content["Key"].removeprefix(self.s3_prefix)
                full_file = urljoin(self.s3_source_location, k)

                await self.db_loader.add_load_job(self.schema, self.table, full_file)
                self.update_completed_task_count(increment_by=1)

    async def _write_list_sql_retry(
        self,
        data_list: List,
        filename: str,
    ):
        """
        Write the given list of data to S3 using CHDB to create, partition, and compress the files.
        """
        # We need to urljoin here because if there are double slashes ClickHouse won't load.
        full_file = urljoin(f"{self.s3_source_location}", f"{filename}.native.lz4")

        # If this is passed, CHDB will create a separate file in S3 for each partition. This is
        # important to allow ClickHouse to load the files without running out of memory.
        partition_statment = (
            "" if not self.partition_key else f"PARTITION BY {self.partition_key}"
        )

        # s3_truncate_on_insert means that CHDB will overwrite existing files instead of failing.
        sql = f"""
        INSERT INTO TABLE FUNCTION
            s3(
                '{full_file}',
                '{self.config["s3_key"]}',
                '{self.config["s3_secret"]}',
                'NATIVE',
                '{self.table_definition}'
            )
            {partition_statment}
            SETTINGS s3_truncate_on_insert=1
            VALUES {",".join(data_list)}
        """

        await self._write_sql_with_retry(sql)

        # Only add the loading job if we are configured to do so.
        if self.load_from_s3_after:
            if self.db_loader and self.schema and self.table:
                # We don't actually get the partitioned filenames back from the insert, so we have
                # to load by wildcard. It is important that the wildcard matches a distinct batch
                # of files that were inserted together, so the filename should be unique to the
                # batch.
                full_file = full_file.format(_partition_id="*")

                await self.db_loader.add_load_job(self.schema, self.table, full_file)
            else:
                raise RuntimeError(
                    "load_from_s3_after is True, but the loader, schema, or table is not set!"
                )

    async def _write_sql_with_retry(self, sql: str):
        """
        Wrap insert commands with a single retry.

        Sometimes the connection randomly dies, this gives us a second shot in that case instead
        of messing up a huge data load for a spurious error.
        """
        # This allows CHDB to run in a separate thread, yielding to other tasks while waiting for
        # the write to complete.
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, chdb.query, sql)
        except Exception as e:
            self.logger.info(f"Retrying due to CHDB error: {e}")
            self.logger.info(f"SQL: {sql}")
            await loop.run_in_executor(None, chdb.query, sql)


class WriteXAPIEvents(XAPILakeCHDBAsync):
    """
    Handles writing of the xAPI events.

    Because there are so many more of these than other types of data, it has a complicated
    queuing and batching system with its own workers that separates the CPU intensive load of
    generating random events, and the IO load of writing to ClickHouse into separate piles that
    can run simultaneously.
    """

    task_name = "Insert xAPI Events"
    table_definition = "event_id String, emission_time DateTime, event String"
    # Year was chosen here to balance the number of files that need to be loaded against the number
    # of partitions in ClickHouse that can be touched by any given batch of data. By default we
    # partition by YYYYMM in Aspects, so in theory no more than 12 partitions should be touched
    # with this. If we find that we get "too many files" errors in ClickHouse, we can change this to
    # toYYYYMM instead, but that slows down the load a fair amount.
    partition_key = "toYear(emission_time)"
    schema = "xapi"
    table = "xapi_events_all"
    s3_load_prefix = "xapi_"

    def __init__(
        self,
        config: Dict,
        logger: Logger,
        event_generator: EventGenerator,
        db_loader: "LoadFromS3|None",
    ):
        super().__init__(config, logger, event_generator, db_loader)
        self.num_xapi_batches = config["num_xapi_batches"]
        self.update_total_task_count(event_generator.get_random_event_count())

    async def _get_batch_sql(self) -> List[str]:
        """
        Get a batch of events from the EventGenerator and format them for CHDB insert.
        """
        out_data = []

        # We can be getting huge amounts of events from the EventGenerator, so we need to
        # process them into the final strings right away to free up that memory. This is a
        # tradeoff, getting events one at a time uses substantially less memory when dealing with
        # large batch sizes, but is much slower than getting them in bulk.
        for v in self.event_generator.get_batch_events():
            out = f"('{v['event_id']}', '{v['emission_time']}', '{v['event']}')"
            out_data.append(out)
        return out_data

    async def _populate_queue(self):
        """
        Create the queue of batch IDs to process.

        The queue for this task is just integers representing the batch IDs. The process task
        actually generates the events and writes them to S3.
        """
        for i in range(self.num_xapi_batches):
            if i % 10 == 0:
                self.logger.info(f"   {self.task_name} enqueuing xAPI batch {i}")

            # This will block this thread if the queue is already full
            await self.queue.put(i)

            # Sometimes Python will not yield this loop to the other tasks, so we need to add a
            # small sleep to give the other threads a turn.
            await asyncio.sleep(0.1)

    async def _process_queue_item(
        self, worker_id: int, batch_id: int, batch: List | int
    ):
        """
        Process a batch of events from the EventGenerator and write them to S3 using CHDB.
        The item on the queue is just a batch ID indicating that there are more events to process.
        This function gets the SQL for a batch of events using a separate *process* pool
        (CPU bound), then writes them to S3 using CHDB, in a separate *thread* (IO bound).
        """
        assert isinstance(batch, int), "Batch must be an int"

        self.logger.debug(f"   {self.task_name} worker generating events for {batch}")

        # Is the process pool for this CPU bound task, this thread will block
        # until it completes. This is important when we're getting, say, a
        # million events at a time.
        with ProcessPoolExecutor():
            out_data = await asyncio.create_task(self._get_batch_sql())

        self.logger.debug(f"   {self.task_name} worker inserting batch {batch}")

        # This is the IO bound part, this thread will block until the write is complete.
        await self._write_list_sql_retry(
            out_data,
            f"xapi_{{_partition_id}}_{batch}",
        )
        self.logger.debug(f"   {self.task_name} worker inserted batch {batch}")


class WriteInitialEnrollments(WriteXAPIEvents):
    """
    A separate task that writes initial xAPI enrollment events.

    This is necessary to ensure that all events make sense, and sets the enrollment dates that
    every event for a user in a particular course can take place on.

    This inherits from WriteXAPIEvents so that it can reuse much of the queue and worker system, but
    instead of the workers pulling randomized batches of events from the event generator, each
    item on the queue is (up to) a batch_size number of enrollment event Dicts. The last batch may
    be smaller than batch_size since the number of enrollments is based on the size of each type
    of course and how many courses are being created.
    """

    task_name = "Insert Initial Enrollments"
    schema = "xapi"
    table = "xapi_events_all"
    # Since WriteXAPIEvents uses the xapi_ prefix, we need to use a prefix that does not begin
    # with xapi_ here, or those events will be loaded twice.
    s3_load_prefix = "initial_enrollments_xapi_"

    async def _populate_queue(self):
        """
        Create the queue of batch IDs to process.

        The queue for this task is just integers representing the batch IDs. The process task
        actually generates the events and writes them to S3.
        """
        # This needs to be here instead of in __init__ since it will be 0 until EventGenerator
        # setup is complete.
        self.update_total_task_count(self.event_generator.get_enrollment_event_count())

        batch = []

        # get_enrollment_events is a generator, so this is not as bad as it could be memory-wise
        # but queue_size * batch_size * dict size can still be a lot of memory. We may eventually
        # want to make the queue size smaller for this task. Currently it inherits from
        # WriteXapiEvents.
        for i in self.event_generator.get_enrollment_events():
            batch.append(i)
            if len(batch) == self.batch_size:
                await self.queue.put(batch)
                batch = []

                # Let other tasks do their work as well
                await asyncio.sleep(0.01)

        # In the very likely event that the last batch is smaller than batch_size, we need to
        # make sure it gets added.
        if batch:
            self.logger.debug("Enqueuing final initial enrollment batch")
            await self.queue.put(batch)

    async def _process_queue_item(
        self, worker_id: int, batch_id: int, batch: List | int
    ):
        """
        Process a batch of events from the EventGenerator and write them to S3 using CHDB.
        The item on the queue is just a batch ID indicating that there are more events to process.
        This function gets the SQL for a batch of events using a separate *process* pool
        (CPU bound), then writes them to S3 using CHDB, in a separate *thread* (IO bound).
        """
        assert isinstance(batch, List), "Batch must be a list"

        out_data = []

        for v in batch:
            out = f"('{v['event_id']}', '{v['emission_time']}', '{v['event']}')"
            out_data.append(out)

        self.logger.debug(
            f"   {self.task_name} worker {worker_id} inserting enrollment batch"
        )
        # This is the IO bound part, this thread will block until the write is complete.
        # The filname this is written to needs to be distinct for each worker/batch, and
        # gets further partitioned by the partition key inherited from WriteXAPIEvents.
        await self._write_list_sql_retry(
            out_data,
            f"initial_enrollments_xapi_{{_partition_id}}_{worker_id}_{batch_id}",
        )
        self.logger.debug(
            f"   {self.task_name} worker {worker_id} inserted batch {batch_id}"
        )


class WriteCourses(XAPILakeCHDBAsync):
    """
    This task is fairly simple, it just takes the course data from the event generator and saves it
    to S3 once for each "num_course_publishes" in the config. Each publish gets its own file.
    """

    task_name = "Insert Courses"
    schema = "event_sink"
    table = "course_overviews"
    s3_load_prefix = "course_overviews_publish_"
    table_definition = """
    `org` String,
    `course_key` String,
    `display_name` String,
    `course_start` String,
    `course_end` String,
    `enrollment_start` String,
    `enrollment_end` String,
    `self_paced` Bool,
    `course_data_json` String,
    `created` String,
    `modified` String,
    `dump_id` UUID,
    `time_last_dumped` String
    """

    async def _run_task(self):
        """
        Insert the course overview data to ClickHouse.

        This allows us to test join performance to get course and block names.
        """
        courses = self.event_generator.courses
        num_course_publishes = self.config["num_course_publishes"]
        self.update_total_task_count(num_course_publishes)

        # Serializing each course repeatedly is expensive, but the alternative is to keep
        # all course data in memory and save it several times, which may be prohibitively bad
        # for memory, or generate one file for each course with every publish in it which can be
        # slow to load to ClickHouse. Currently this is one of the faster tasks so it has
        # been low priority to optimize.
        for i in range(num_course_publishes):
            out_data = []
            for course in courses:
                c = course.serialize_course_data_for_event_sink()
                dump_id = str(uuid.uuid4())
                dump_time = datetime.now(UTC)
                try:
                    out = f"""(
                        '{c["org"]}',
                        '{c["course_key"]}',
                        '{c["display_name"]}',
                        '{c["course_start"]}',
                        '{c["course_end"]}',
                        '{c["enrollment_start"]}',
                        '{c["enrollment_end"]}',
                        '{c["self_paced"]}',
                        '{c["course_data_json"]}',
                        '{c["created"]}',
                        '{c["modified"]}',
                        '{dump_id}',
                        '{dump_time}'
                    )"""
                    out_data.append(out)
                except Exception:
                    self.logger.info(c)
                    raise

            await self._write_list_sql_retry(out_data, f"course_overviews_publish_{i}")
            self.update_completed_task_count(increment_by=1)


class WriteBlocks(XAPILakeCHDBAsync):
    """
    Write the course block data to S3 using CHDB.
    """

    task_name = "Insert Blocks"
    schema = "event_sink"
    table = "course_blocks"
    s3_load_prefix = "course_blocks_"
    table_definition = """
    `org` String,
    `course_key` String,
    `location` String,
    `display_name` String,
    `xblock_data_json` String,
    `order` Int32 DEFAULT 0,
    `edited_on` String,
    `dump_id` UUID,
    `time_last_dumped` String
    """

    async def _run_task(self):
        """
        Write the course blocks once per course publish.

        Because there are so many more blocks than courses, we do split this up into one file
        per course.
        """
        self.logger.info("Blocks started")
        courses = self.event_generator.courses
        num_course_publishes = self.config["num_course_publishes"]
        self.update_total_task_count(len(courses))

        course_count = 0
        for course in courses:
            course_count += 1
            out_data = []
            blocks = course.serialize_block_data_for_event_sink()

            for i in range(num_course_publishes):
                dump_id = str(uuid.uuid4())
                dump_time = datetime.now(UTC)
                for b in blocks:
                    try:
                        out = f"""(
                            '{b["org"]}',
                            '{b["course_key"]}',
                            '{b["location"]}',
                            '{b["display_name"]}',
                            '{b["xblock_data_json"]}',
                            '{b["order"]}',
                            '{b["edited_on"]}',
                            '{dump_id}',
                            '{dump_time}'
                        )"""
                        out_data.append(out)
                    except Exception:
                        self.logger.info(b)
                        raise

            await self._write_list_sql_retry(
                out_data,
                f"course_blocks_{course_count}",
            )

            self.update_completed_task_count(increment_by=1)


class WriteObjectTags(XAPILakeCHDBAsync):
    """
    Write the tags for each block / object to S3 using CHDB.
    """

    task_name = "Insert Object Tags"
    schema = "event_sink"
    table = "object_tag"
    s3_load_prefix = "object_tag_"
    table_definition = """
    `id` Int32,
    `object_id` String,
    `taxonomy` Int32,
    `tag` Int32,
    `_value` String,
    `_export_id` String,
    `lineage` String,
    `dump_id` UUID,
    `time_last_dumped` String
    """

    async def _run_task(self):
        """
        Insert the object_tag data to ClickHouse.

        Most of the work for this is done in insert_event_sink_block_data
        """
        self.logger.info("Object tags started")
        obj_tag_out_data = []
        num_course_publishes = self.config["num_course_publishes"]
        self.update_total_task_count(len(self.event_generator.courses))

        # We write out one file per course, with all course publishes in it due to the number
        # of tags * blocks that can be in a course.
        course_count = 0
        for course in self.event_generator.courses:
            course_count += 1
            obj_tag_out_data = []
            object_tags = course.serialize_object_tag_data_for_event_sink()

            row_id = 0
            for i in range(num_course_publishes):
                dump_id = str(uuid.uuid4())
                dump_time = datetime.now(UTC)
                for obj_tag in object_tags:
                    row_id += 1

                    out_tag = f"""(
                    {row_id},
                    '{obj_tag["object_id"]}',
                    {obj_tag["taxonomy_id"]},
                    {obj_tag["tag_id"]},
                    '{obj_tag["value"]}',
                    'fake export id',
                    '{obj_tag["hierarchy"]}',
                    '{dump_id}',
                    '{dump_time}'
                    )"""

                    obj_tag_out_data.append(out_tag)

            await self._write_list_sql_retry(
                obj_tag_out_data,
                f"object_tag_{course_count}",
            )

            self.update_completed_task_count(increment_by=1)


class WriteTaxonomies(XAPILakeCHDBAsync):
    """
    Write the taxonomies to S3 using CHDB.
    """

    task_name = "Insert Taxonomies"
    schema = "event_sink"
    table = "taxonomy"
    s3_load_prefix = "taxonomy"
    table_definition = """
    `id` Int32,
    `name` String,
    `dump_id` UUID,
    `time_last_dumped` String
    """

    async def _run_task(self):
        """
        Insert the taxonomies into the event sink db.
        """
        taxonomies = self.event_generator.taxonomies
        self.update_total_task_count(len(taxonomies))

        dump_id = str(uuid.uuid4())
        dump_time = datetime.now(UTC)
        out_data = []

        for taxonomy in taxonomies.keys():
            out = f"""(
                {self.completed_task_count},
                '{taxonomy}',
                '{dump_id}',
                '{dump_time}'
            )
            """
            out_data.append(out)
            self.update_completed_task_count(increment_by=1)

        await self._write_list_sql_retry(out_data, "taxonomy")


class WriteTags(XAPILakeCHDBAsync):
    """
    Write the tags for each taxonomy to S3 using CHDB.
    """

    task_name = "Insert Tags"
    schema = "event_sink"
    table = "tag"
    s3_load_prefix = "tag"
    table_definition = """
    `id` Int32,
    `taxonomy` Int32,
    `parent` Int32,
    `value` String,
    `external_id` String,
    `lineage` String,
    `dump_id` UUID,
    `time_last_dumped` String
    """

    async def _run_task(self):
        """
        Insert the tags into the event sink db.
        """
        tags = self.event_generator.tags
        self.update_total_task_count(len(tags))

        dump_id = str(uuid.uuid4())
        dump_time = datetime.now(UTC)
        tag_out_data = []

        for tag in tags:
            out_tag = f"""(
                {tag["tag_id"]},
                {tag["taxonomy_id"]},
                {tag["parent_int_id"] or 0},
                '{tag["value"]}',
                '{tag["id"]}',
                '{tag["hierarchy"]}',
                '{dump_id}',
                '{dump_time}'
            )"""

            tag_out_data.append(out_tag)
            self.update_completed_task_count(increment_by=1)

        await self._write_list_sql_retry(tag_out_data, "tag")


class WriteExternalIDs(XAPILakeCHDBAsync):
    """
    Write the user external IDs to S3 using CHDB.
    """

    task_name = "Insert ExternalIDs"
    schema = "event_sink"
    table = "external_id"
    s3_load_prefix = "external_id"
    table_definition = """
    `external_user_id` UUID,
    `external_id_type` String,
    `username` String,
    `user_id` Int32,
    `dump_id` UUID,
    `time_last_dumped` String
    """

    async def _run_task(self):
        """
        Write the external IDs to S3.

        These are immutable so only get written once.
        """
        actors = self.event_generator.actors
        self.update_total_task_count(len(actors))
        out_external_id = []

        for actor in actors:
            dump_id = str(uuid.uuid4())
            dump_time = datetime.now(UTC)
            id_row = f"""(
                '{actor.id}',
                'xapi',
                '{actor.username}',
                '{actor.user_id}',
                '{dump_id}',
                '{dump_time}'
            )"""
            out_external_id.append(id_row)
            self.update_completed_task_count(increment_by=1)

        await self._write_list_sql_retry(out_external_id, "external_id")


class WriteUserProfiles(XAPILakeCHDBAsync):
    """
    Write the user profile data to S3 using CHDB.
    """

    task_name = "Insert User Profiles"
    schema = "event_sink"
    table = "user_profile"
    s3_load_prefix = "user_profile_"
    table_definition = """
    `id` Int32,
    `user_id` Int32,
    `name` String,
    `username` String,
    `email` String,
    `meta` String,
    `courseware` String,
    `language` String,
    `location` String,
    `year_of_birth` String,
    `gender` String,
    `level_of_education` String,
    `mailing_address` String,
    `city` String,
    `country` String,
    `state` String,
    `goals` String,
    `bio` String,
    `profile_image_uploaded_at` String,
    `phone_number` String,
    `dump_id` UUID,
    `time_last_dumped` String
    """

    async def _run_task(self):
        """
        Write the user profile data to S3 once per actor * num_actor_profile_changes.
        """
        actors = self.event_generator.actors
        num_actor_profile_changes = self.config["num_actor_profile_changes"]
        self.update_total_task_count(num_actor_profile_changes)

        # Similar to WriteCourses, we need to serialize the actor data for each profile change
        # and are serializing each user num_actor_profile_changes times. This is a tradeoff between
        # doing that work several times and keeping the number of files smaller.
        #
        # This can probably be sped up quite a bit and use a lot less memory by using a batching
        # process that only serializes the actor once and creates a moderate number of files.
        for i in range(num_actor_profile_changes):
            out_profiles = []
            self.logger.info(f"   Actor save round {i} - {datetime.now().isoformat()}")

            for actor in actors:
                dump_id = str(uuid.uuid4())
                dump_time = datetime.now(UTC)

                # This first column is usually the MySQL row pk, we just
                # user this for now to have a unique id.
                out_profiles.append(f"""(
                    '{actor.user_id}',
                    '{actor.user_id}',
                    '{actor.name}',
                    '{actor.username}',
                    '{actor.username}@aspects.invalid',
                    '{actor.meta}',
                    '{actor.courseware}',
                    '{actor.language}',
                    '{actor.location}',
                    '{actor.year_of_birth}',
                    '{actor.gender}',
                    '{actor.level_of_education}',
                    '{actor.mailing_address}',
                    '{actor.city}',
                    '{actor.country}',
                    '{actor.state}',
                    '{actor.goals}',
                    '{actor.bio}',
                    '{actor.profile_image_uploaded_at}',
                    '{actor.phone_number}',
                    '{dump_id}',
                    '{dump_time}'
                )""")

            await self._write_list_sql_retry(
                out_profiles,
                f"user_profile_{i}",
            )

            self.update_completed_task_count(increment_by=1)


class LoadFromS3(XAPILakeCHDBAsync):
    """
    This task is responsible for loading the data files from S3 into ClickHouse.

    It maintains a queue of filename wildcards, which are either added by the other tasks during
    load-db (when chdb_load_from_s3_after is True) or are fed to it by looping through the S3 bucket
    and prefix in XAPILakeCHDBAsync._run_db_load_task and adding every file to the queue.
    """

    task_name = "Load From S3"
    error_count = 0
    shutting_down = False

    def __init__(
        self,
        config: Dict,
        logger: Logger,
        event_generator: EventGenerator,
        db_loader: "LoadFromS3|None",
    ):
        super().__init__(config, logger, event_generator, db_loader)
        # This queue doesn't have a maxsize because we don't want to block on adding files to it,
        # and the amount of memory used for each entry is fairly small, just a dict of filename,
        # ClickHouse schema name, and ClickHouse table name to write to. Almost certainly smaller
        # than the amount of memory the thread we'd be blocking is holding on to.
        self.queue = asyncio.Queue()
        self.error_count_lock = Lock()

        # We maintain a list of other running tasks that may be adding new files to our queue. Only
        # when all other tasks are complete and the queue is drained can we shut down.
        self.registered_tasks = []

    def reset(self):
        super().reset()
        self.error_count = 0
        self.shutting_down = False

    def register_task(self, task: Waiter):
        """
        As each task spins up, it adds itself to our list of tasks to wait for.
        """
        # Don't add ourself, or we'll never finish.
        if task != self:
            self.registered_tasks.append(task)

    def update_error_count(self):
        """
        Thread safe error count update.
        """
        with self.error_count_lock:
            self.error_count += 1

    async def add_load_job(self, schema: str, table_name: str, file_path: str):
        """
        Other tasks call this to add a job to the queue
        """
        self.update_total_task_count(increment_by=1)
        self.logger.debug(f"Adding DB load job {self.total_task_count}")
        await self.queue.put(
            {
                "job_id": self.total_task_count,
                "schema": schema,
                "table_name": table_name,
                "file_path": file_path,
            }
        )

    async def _populate_queue(self):
        """
        This task doesn't directly populate its queue, it just waits while other tasks add
        data load tasks to its queue.
        """
        # Make sure we have a ClickHouse connection to work with
        if not self.client:
            await self.set_client()

        while not self.shutting_down:
            # Just idle until all of the tasks that may be feeding us files are done
            if all([t.finished for t in self.registered_tasks]):
                self.logger.info("All LoadFromS3 tasks complete, shutting down.")
                self.shutting_down = True

            await asyncio.sleep(1)

    async def _process_queue_item(self, worker_id: int, batch_id: int, batch: Any):
        """
        This function is a worker thread that runs asynchronously. The item on the queue
        is a dict of S3 location, CH schema, and CH table to write to. The workers
        run insert SQL in ClickHouse, which will cause it to execute the load.
        """
        assert isinstance(batch, dict), "Batch must be a dict"

        try:
            await self._load_file_to_clickhouse(
                batch["schema"], batch["table_name"], batch["file_path"]
            )
        except Exception as e:
            self.update_error_count()
            self.logger.error(batch)
            self.logger.error(
                f"   DB load worker encountered an error: {worker_id}, job {batch['job_id']}.",
                exc_info=e,
            )
        finally:
            self.update_completed_task_count(increment_by=1)

    async def _load_file_to_clickhouse(
        self, schema: str, table_name: str, file_path: str
    ):
        """
        Load a file from S3 to ClickHouse.
        """
        self.logger.debug(f"Inserting into {schema}.{table_name} from {file_path}")

        sql = f"""
        INSERT INTO {schema}.{table_name}
           SELECT *
           FROM s3('{file_path}', '{self.s3_key}', '{self.s3_secret}', 'NATIVE');
        """

        # This is the IO bound part, this thread will block until the write is complete.
        await self._insert_sql_with_retry(sql)

    async def _run_db_load_task(self):
        """
        When we're just running a load from S3 and not generating new files this gets called.

        In the case of this task, it's the same as _run_task.
        """
        return await self._run_task()
