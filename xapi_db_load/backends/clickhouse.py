"""
Asynchronouse ClickHouse backend.

Inserts data to ClickHouse directly using SQL strings. This backend is fairly limited and slow
compared to the others, but has the fewest dependencies.
"""

import uuid
from datetime import UTC, datetime
from logging import Logger
from typing import Any, List

from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

from xapi_db_load.backends.base_async_backend import (
    BaseBackendTasks,
    QueueBackend,
)
from xapi_db_load.generate_load_async import EventGenerator
from xapi_db_load.waiter import Waiter


class AsyncClickHouseTasks(BaseBackendTasks):
    def __repr__(self) -> str:
        return f"AsyncClickHouseTasks: {self.config['db_host']}"

    def get_test_data_tasks(self) -> List[Waiter]:
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
            InsertXAPIEvents(self.config, self.logger, self.event_generator),
        ]


class XAPILakeClickhouseAsync(QueueBackend):
    """
    Abstract implementation for ClickHouse async.
    """

    async def _insert_rows(
        self,
        table: str,
        rows: List[tuple],
        column_names: List[str] | str = "*",
        database: str = "event_sink",
    ):
        """Insert rows via clickhouse-connect's parameterized insert."""
        try:
            if not self.client:
                await self.set_client()
            assert self.client
            await self.client.insert(
                table, rows, column_names=column_names, database=database
            )
        except OperationalError as e:
            self.logger.error(e)
            self.logger.error("ClickHouse OperationalError, retrying once.")
            await self.set_client()
            assert self.client
            await self.client.insert(
                table, rows, column_names=column_names, database=database
            )
        except DatabaseError as e:
            self.logger.error("ClickHouse DatabaseError: %s", e)
            raise


class InsertXAPIEvents(XAPILakeClickhouseAsync):
    """
    Handles inserting the xAPI events.

    Because there are so many more of these than other types of data, it has a complicated
    queuing and batching system with its own workers that separates the CPU intensive load of
    generating random events, and the IO load of writing to ClickHouse into separate piles that
    can run simultaneously.
    """

    task_name = "Insert xAPI Events"

    def __init__(self, config: dict, logger: Logger, event_generator: EventGenerator):
        super().__init__(config, logger, event_generator)
        self.num_xapi_batches = config["num_xapi_batches"]
        # Our unit of measure for task completion is the number of batches to process
        self.update_total_task_count(self.num_xapi_batches)

    async def _populate_queue(self):
        for i in range(self.num_xapi_batches):
            if i % 10 == 0:
                self.logger.debug(f"   {self.task_name} enquing batch {i}")

            # This will block if the queue is at maxsize, but that's fine
            await self.queue.put(i)
        self.logger.debug(f"   {self.task_name} finished queueing batches")

    async def _process_queue_item(self, worker_id: int, batch_id: int, batch: Any):
        """
        Process a batch of data for insertion.
        """
        assert isinstance(batch, int), f"Batch for {self.task_name} must be an int."
        out_data = []

        self.logger.debug(
            f"   {self.task_name} worker gathering events for batch {batch}"
        )
        # For this class it's more memory efficient to convert to strings as they
        # come in rather than enumerate the whole batch later.
        for v in self.event_generator.get_batch_events():
            out_data.append(self._format_row(v))

        self.logger.debug(f"   {self.task_name} worker inserting {batch}")
        await self._do_insert(out_data)
        self.logger.debug(f"   {self.task_name} worker batch {batch} inserted")
        self.update_completed_task_count(increment_by=1)

    async def _do_insert(self, out_data: List[tuple]):
        """
        Performs the actual insert of a batch of events to ClickHouse.
        """
        await self._insert_rows(
            "xapi_events_all",
            out_data,
            column_names=["event_id", "emission_time", "event"],
            database="xapi",
        )

    def _format_row(self, row: dict) -> tuple:
        """
        Format a row of data for ClickHouse insert.

        This is broken out so it can be overridden in the Ralph backend.
        """
        return (row["event_id"], row["emission_time"], row["event"])


class InsertInitialEnrollments(InsertXAPIEvents):
    """
    A separate task that writes initial xAPI enrollment events.

    This is necessary to ensure that all events make sense, and sets the enrollment dates that
    every event for a user in a particular course can take place on.

    This inherits from InsertXAPIEvents so that it can reuse much of the queue and worker system,
    but instead of the workers pulling randomized batches of events from the event generator, each
    item on the queue is (up to) a batch_size number of enrollment event Dicts. The last batch may
    be smaller than batch_size since the number of enrollments is based on the size of each type
    of course and how many courses are being created.
    """

    task_name = "Insert Initial Enrollments"

    async def _process_queue_item(self, worker_id: int, batch_id: int, batch: Any):
        """
        Process a batch of data for insertion.
        """
        assert isinstance(batch, List), f"Batch for {self.task_name} must be a list."
        out_data = []

        self.logger.debug(
            f"   {self.task_name} worker formatting events for batch length {batch_id}"
        )
        # For this class it's more memory efficient to convert to strings as they
        # come in rather than enumerate the whole batch later.
        for v in batch:
            out_data.append(self._format_row(v))

        self.logger.debug(f"   {self.task_name} worker inserting {batch_id}")
        await self._do_insert(out_data)
        self.logger.debug(f"   {self.task_name} worker batch inserted {batch_id}")
        self.update_completed_task_count(increment_by=len(out_data))

    async def _populate_queue(self):
        """
        Populate the queue with enrollment events.

        This is a separate queue from the xAPI events queue, and is populated
        with enrollment events rather than random events.
        """
        self.logger.debug(f"{self.task_name} worker populating queue")

        # This needs to be here instead of in __init__ since it will be 0 until EventGenerator
        # setup is complete.
        self.update_total_task_count(self.event_generator.get_enrollment_event_count())

        # get_enrollment_events is a generator, so this is not as bad as it could be memory-wise
        # but queue_size * batch_size * dict size can still be a lot of memory. We may eventually
        # want to make the queue size smaller for this task.
        batch = []
        for i in self.event_generator.get_enrollment_events():
            batch.append(i)
            if len(batch) == self.batch_size:
                self.logger.debug(
                    f"   {self.task_name} enqueuing enrollment batch at event {i}"
                )
                await self.queue.put(batch)
                batch = []

        # In the very likely event that the last batch is smaller than batch_size, we need to
        # make sure it gets added.
        if batch:
            self.logger.debug(
                f"   {self.task_name} queueing final initial enrollment batch"
            )
            await self.queue.put(batch)

        self.logger.info(f"   {self.task_name} worker queue populated")


class InsertCourses(XAPILakeClickhouseAsync):
    task_name = "Insert Courses"

    async def _run_task(self):
        """
        Insert the course overview data to ClickHouse.
        """
        courses = self.event_generator.courses
        num_course_publishes = self.config["num_course_publishes"]
        self.update_total_task_count(num_course_publishes)

        # Currently this does one insert per course, of a number of rows equal to
        # the num_course_publishes setting. This could be improved by instead
        # inserting larger batches of rows at the cost of some memory.
        for i in range(num_course_publishes):
            if i % 10 == 0:
                self.logger.info(
                    f"   {self.task_name} starting publish {i} - {datetime.now().isoformat()}"
                )

            out_data = []
            for course in courses:
                c = course.serialize_course_data_for_event_sink()
                dump_id = str(uuid.uuid4())
                dump_time = datetime.now(UTC)
                out_data.append(
                    (
                        c["org"],
                        c["course_key"],
                        c["display_name"],
                        c["course_start"],
                        c["course_end"],
                        c["enrollment_start"],
                        c["enrollment_end"],
                        c["self_paced"],
                        c["course_data_json"],
                        c["created"],
                        c["modified"],
                        dump_id,
                        dump_time,
                    )
                )

            await self._insert_rows("course_overviews", out_data)
            self.update_completed_task_count(increment_by=1)


class InsertBlocks(XAPILakeClickhouseAsync):
    task_name = "Insert Blocks"

    async def _run_task(self):
        """
        Insert the block data to ClickHouse.
        """
        self.logger.info("Blocks started")
        courses = self.event_generator.courses
        num_course_publishes = self.config["num_course_publishes"]
        self.update_total_task_count(len(courses) * num_course_publishes)

        course_count = 0
        for course in courses:
            course_count += 1
            out_data = []
            blocks = course.serialize_block_data_for_event_sink()

            for i in range(num_course_publishes):
                dump_id = str(uuid.uuid4())
                dump_time = datetime.now(UTC)
                for b in blocks:
                    out_data.append(
                        (
                            b["org"],
                            b["course_key"],
                            b["location"],
                            b["display_name"],
                            b["xblock_data_json"],
                            b["order"],
                            b["edited_on"],
                            dump_id,
                            dump_time,
                        )
                    )

                self.logger.debug(
                    f"   {self.task_name} starting insert for course {course_count}"
                )
                await self._insert_rows("course_blocks", out_data)
                self.update_completed_task_count(increment_by=1)


class InsertObjectTags(XAPILakeClickhouseAsync):
    task_name = "Insert ObjectTags"

    async def _run_task(self):
        self.logger.info("ObjectTags started")
        courses = self.event_generator.courses
        num_course_publishes = self.config["num_course_publishes"]
        self.update_total_task_count(len(courses) * num_course_publishes)

        for course in courses:
            self.logger.info(
                f"   {self.task_name} starting insert for course {self.completed_task_count}"
            )
            object_tags = course.serialize_object_tag_data_for_event_sink()

            for i in range(num_course_publishes):
                dump_id = str(uuid.uuid4())
                dump_time = datetime.now(UTC)
                obj_tag_out_data = []

                row_id = 0
                for obj_tag in object_tags:
                    row_id += 1
                    obj_tag_out_data.append(
                        (
                            row_id,
                            obj_tag["object_id"],
                            obj_tag["taxonomy_id"],
                            obj_tag["tag_id"],
                            obj_tag["value"],
                            "fake export id",
                            obj_tag["hierarchy"],
                            dump_id,
                            dump_time,
                        )
                    )

                await self._insert_rows("object_tag", obj_tag_out_data)
                self.update_completed_task_count(increment_by=1)


class InsertTaxonomies(XAPILakeClickhouseAsync):
    task_name = "Insert Taxonomies"

    async def _run_task(self):
        """
        Insert the taxonomies into ClickHouse
        """
        dump_id = str(uuid.uuid4())
        dump_time = datetime.now(UTC)
        taxonomies = self.event_generator.taxonomies

        self.update_total_task_count(len(taxonomies))

        out_data = []
        id = 0
        for taxonomy in taxonomies.keys():
            id += 1
            out_data.append((id, taxonomy, dump_id, dump_time))
            self.update_completed_task_count(increment_by=1)

        await self._insert_rows("taxonomy", out_data)


class InsertTags(XAPILakeClickhouseAsync):
    task_name = "Insert Tags"

    async def _run_task(self):
        """
        Insert the tags into ClickHouse.
        """
        tags = self.event_generator.tags

        # We only do one insert for this task
        self.update_total_task_count(1)

        dump_id = str(uuid.uuid4())
        dump_time = datetime.now(UTC)
        tag_out_data = []

        for tag in tags:
            tag_out_data.append(
                (
                    tag["tag_id"],
                    tag["taxonomy_id"],
                    tag["parent_int_id"] or 0,
                    tag["value"],
                    tag["id"],
                    tag["hierarchy"],
                    dump_id,
                    dump_time,
                )
            )

        await self._insert_rows("tag", tag_out_data)
        self.update_completed_task_count(increment_by=1)


class InsertExternalIDs(XAPILakeClickhouseAsync):
    task_name = "Insert ExternalIDs"

    async def _run_task(self):
        """
        Write the external ids to ClickHouse.

        These are immutable so only need to be written once, but inserts are batched.
        """
        actors = self.event_generator.actors
        self.update_total_task_count(len(actors))

        out_external_id = []

        actor_cnt = 0
        for actor in actors:
            actor_cnt += 1
            dump_id = str(uuid.uuid4())
            dump_time = datetime.now(UTC)
            out_external_id.append(
                (
                    actor.id,
                    "xapi",
                    actor.username,
                    actor.user_id,
                    dump_id,
                    dump_time,
                )
            )

            if len(out_external_id) == self.config["batch_size"]:
                if actor_cnt % 100 == 0:
                    self.logger.debug(
                        f"   {self.task_name} starting insert for external ids batch {actor_cnt}"
                    )
                await self._insert_rows("external_id", out_external_id)
                self.update_completed_task_count(increment_by=len(out_external_id))
                out_external_id = []

        # Catch any stragglers from the last batch
        if len(out_external_id):
            await self._insert_rows("external_id", out_external_id)
            self.update_completed_task_count(increment_by=len(out_external_id))


class InsertProfiles(XAPILakeClickhouseAsync):
    task_name = "Insert Profiles"

    async def _run_task(self):
        """
        Insert the user profile data to ClickHouse once per actor * num_actor_profile_changes.

        Inserts are batched.
        """
        actors = self.event_generator.actors
        num_actor_profile_changes = self.config["num_actor_profile_changes"]
        self.update_total_task_count(
            len(actors) + (len(actors) * num_actor_profile_changes)
        )
        out_profile = []

        for i in range(num_actor_profile_changes):
            self.logger.debug(
                f"   {self.task_name} starting insert for user profiles round {i}"
            )

            for actor in actors:
                dump_id = str(uuid.uuid4())
                dump_time = datetime.now(UTC)

                # This first column is usually the MySQL row pk, we just
                # use this for now to have a unique id.
                out_profile.append(
                    (
                        actor.user_id,
                        actor.user_id,
                        actor.name,
                        actor.username,
                        f"{actor.username}@aspects.invalid",
                        actor.meta,
                        actor.courseware,
                        actor.language,
                        actor.location,
                        actor.year_of_birth,
                        actor.gender,
                        actor.level_of_education,
                        actor.mailing_address,
                        actor.city,
                        actor.country,
                        actor.state,
                        actor.goals,
                        actor.bio,
                        actor.profile_image_uploaded_at,
                        actor.phone_number,
                        dump_id,
                        dump_time,
                    )
                )

                if len(out_profile) == self.config["batch_size"]:
                    await self._insert_rows("user_profile", out_profile)
                    self.update_completed_task_count(increment_by=len(out_profile))
                    out_profile = []

        # Catch any stragglers from the last batch
        if len(out_profile):
            await self._insert_rows("user_profile", out_profile)
            self.update_completed_task_count(increment_by=len(out_profile))
