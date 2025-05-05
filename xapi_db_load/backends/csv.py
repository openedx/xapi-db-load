"""
Asynchronouse CSV backend.

Writes gzipped CSV files to local or cloud storage. If using cloud storage, the files can then be
automatically loaded into ClickHouse using the `s3` table function.

This is currently the fastest way to load medium amounts (10s of millions of rows) of data, but
slower than CHDB for large (hundreds of millions of rows) datasets. This can also fail on large
datasets or datasets with long date ranges due to the "too many parts" error. The problem is that
this backend does not have any partitioning or ordering of events the way that CHDB does, each task
only generates one file.
"""

import asyncio
import csv
import os
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import List
from urllib.parse import urljoin

import boto3
from smart_open import open as smart

from xapi_db_load.backends.base_async_backend import (
    BaseBackendTasks,
    BaseClickhouseBackend,
)
from xapi_db_load.waiter import Waiter


class AsyncCSVTasks(BaseBackendTasks):
    def __repr__(self) -> str:
        return f"AsyncCSVTasks: {self.config.get('csv_output_destination')} -> {self.config.get('db_host', 'No ClickHouse configured')}"

    def get_test_data_tasks(self) -> List[Waiter]:
        """
        Return the tasks to be run.
        """
        return [
            self.event_generator,
            WriteCSVCourses(self.config, self.logger, self.event_generator),
            WriteCSVBlocks(self.config, self.logger, self.event_generator),
            WriteCSVObjectTags(self.config, self.logger, self.event_generator),
            WriteCSVTaxonomies(self.config, self.logger, self.event_generator),
            WriteCSVTags(self.config, self.logger, self.event_generator),
            WriteCSVExternalIds(self.config, self.logger, self.event_generator),
            WriteCSVProfiles(self.config, self.logger, self.event_generator),
            WriteCSVXAPIEvents(self.config, self.logger, self.event_generator),
        ]


class XAPILakeCSVAsync(BaseClickhouseBackend):
    """
    Generic Async CSV implementation
    """

    schema = None
    table = None
    filename = None

    def __init__(self, config, logger, event_generator):
        super().__init__(config, logger, event_generator)
        # output_destination can be anything that smart_open can write to.
        # See: https://github.com/piskvorky/smart_open
        self.output_destination = config["csv_output_destination"]

    async def _run_task(self):
        raise NotImplementedError("You must implement the task method")

    @asynccontextmanager
    async def get_csv_handle(self, file_type):
        """
        Get a CSV file handle and writer for writing.

        The handle will be automatically closed when the context manager exits and if we are
        configured to load from S3, the file will be loaded into the database once the handle is
        closed.
        """

        out_filepath = os.path.join(self.output_destination, f"{file_type}.csv.gz")
        os.makedirs(self.output_destination, exist_ok=True)
        transport_params = None
        if self.output_destination.startswith("s3://"):
            session = boto3.Session(
                aws_access_key_id=self.config["s3_key"],
                aws_secret_access_key=self.config["s3_secret"],
            )

            transport_params = {"client": session.client("s3")}
        file_handle = smart(
            out_filepath, "w", compression=".gz", transport_params=transport_params
        )

        try:
            yield csv.writer(file_handle)
        finally:
            file_handle.close()
            if self.load_from_s3_after:
                await self._run_db_load_task()

    async def _run_db_load_task(self):
        """
        Insert the CSV file for this task into the database.

        This task is run when `--load_db_only` is passed to the `load-db` command or when the
        load_from_s3_after setting is True.
        """
        if not self.schema or not self.table or not self.filename:
            raise ValueError(
                f"You must set the schema, table and filename for {self.task_name}"
            )

        file = urljoin(f"{self.config['s3_source_location']}", f"{self.filename}")
        self.logger.info(f"Inserting from {file} into {self.table}")
        await self.set_client()

        sql = f"""
        INSERT INTO {self.schema}.{self.table}
            SELECT *
            FROM s3('{file}', '{self.config["s3_key"]}', '{self.config["s3_secret"]}', 'CSV');
        """
        assert self.client
        await self.client.command(sql)


class WriteCSVXAPIEvents(XAPILakeCSVAsync):
    task_name = "Write CSV xAPI"
    schema = "xapi"
    table = "xapi_events_all"
    filename = "xapi.csv.gz"

    async def _run_task(self):
        """
        Write a batch of rows to the CSV.
        """
        async with self.get_csv_handle("xapi") as xapi_csv_writer:
            self.update_total_task_count(
                self.event_generator.get_random_event_count()
                + self.event_generator.get_enrollment_event_count()
            )

            self.logger.info("Starting enrollment events")

            # First handle the initial enrollments
            for v in self.event_generator.get_enrollment_events():
                xapi_csv_writer.writerow(
                    (v["event_id"], v["emission_time"], str(v["event"]))
                )
                self.update_completed_task_count(increment_by=1)

            self.logger.info("Enrollment events done, starting random events")

            # Now the randomized event batches
            num_batches = self.config["num_xapi_batches"]

            for x in range(num_batches):
                if x % 100 == 0:
                    self.logger.info(f"{x} of {num_batches}")
                    # This task can be very long running and prevent other tasks from
                    # running, so we sleep a bit to let other tasks run.
                    await asyncio.sleep(0.01)

                for v in self.event_generator.get_batch_events():
                    xapi_csv_writer.writerow(
                        (v["event_id"], v["emission_time"], str(v["event"]))
                    )

                self.update_completed_task_count(increment_by=self.batch_size)


class WriteCSVCourses(XAPILakeCSVAsync):
    task_name = "Write CSV Courses"
    schema = "event_sink"
    table = "course_overviews"
    filename = "courses.csv.gz"

    async def _run_task(self):
        """
        Write the course overview data.
        """
        async with self.get_csv_handle("courses") as course_csv_writer:
            courses = self.event_generator.courses
            num_course_publishes = self.config["num_course_publishes"]
            self.update_total_task_count(len(courses) * num_course_publishes)

            for i in range(num_course_publishes):
                if i % 10 == 0:
                    self.logger.debug(f"   Course publish {i}")

                for course in courses:
                    c = course.serialize_course_data_for_event_sink()
                    dump_id = str(uuid.uuid4())
                    dump_time = datetime.now(UTC)
                    course_csv_writer.writerow(
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
                    self.update_completed_task_count(increment_by=1)


class WriteCSVBlocks(XAPILakeCSVAsync):
    task_name = "Write CSV Blocks"
    schema = "event_sink"
    table = "course_blocks"
    filename = "blocks.csv.gz"

    async def _run_task(self):
        """
        Write out the block data file.
        """
        async with self.get_csv_handle("blocks") as blocks_csv_writer:
            courses = self.event_generator.courses
            num_course_publishes = self.config["num_course_publishes"]
            self.update_total_task_count(increment_by=len(courses))

            for course in courses:
                blocks = course.serialize_block_data_for_event_sink()

                for i in range(num_course_publishes):
                    dump_id = str(uuid.uuid4())
                    dump_time = datetime.now(UTC)
                    for b in blocks:
                        blocks_csv_writer.writerow(
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

                self.update_completed_task_count(increment_by=1)


class WriteCSVObjectTags(XAPILakeCSVAsync):
    task_name = "Write CSV ObjectTags"
    schema = "event_sink"
    table = "object_tag"
    filename = "object_tags.csv.gz"

    async def _run_task(self):
        """
        Write out the block data file.
        """
        async with self.get_csv_handle("object_tags") as object_tag_csv_writer:
            num_course_publishes = self.config["num_course_publishes"]
            self.update_total_task_count(
                len(self.event_generator.courses) * num_course_publishes
            )

            row_id = 0
            for course in self.event_generator.courses:
                object_tags = course.serialize_object_tag_data_for_event_sink()

                for i in range(num_course_publishes):
                    dump_id = str(uuid.uuid4())
                    dump_time = datetime.now(UTC)
                    for obj_tag in object_tags:
                        row_id += 1
                        object_tag_csv_writer.writerow(
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

                    self.update_completed_task_count(increment_by=1)


class WriteCSVTaxonomies(XAPILakeCSVAsync):
    task_name = "Write CSV Taxonomies"
    schema = "event_sink"
    table = "taxonomy"
    filename = "taxonomies.csv.gz"

    async def _run_task(self):
        """
        Write out the taxonomies data file.
        """
        async with self.get_csv_handle("taxonomies") as taxonomy_csv_writer:
            taxonomies = self.event_generator.taxonomies
            self.update_total_task_count(len(taxonomies))

            dump_id = str(uuid.uuid4())
            dump_time = datetime.now(UTC)

            id = 1
            for taxonomy in taxonomies.keys():
                id += 1
                taxonomy_csv_writer.writerow((id, taxonomy, dump_id, dump_time))
                self.update_completed_task_count(increment_by=1)


class WriteCSVTags(XAPILakeCSVAsync):
    task_name = "Write CSV Tags"
    schema = "event_sink"
    table = "tag"
    filename = "tags.csv.gz"

    async def _run_task(self):
        """
        Insert the tags into the event sink db.
        """
        async with self.get_csv_handle("tags") as tag_csv_writer:
            tags = self.event_generator.tags
            self.update_total_task_count(len(tags))

            dump_id = str(uuid.uuid4())
            dump_time = datetime.now(UTC)

            for tag in tags:
                tag_csv_writer.writerow(
                    (
                        tag["tag_id"],
                        tag["taxonomy_id"],
                        tag["parent_int_id"],
                        tag["value"],
                        tag["id"],
                        tag["hierarchy"],
                        dump_id,
                        dump_time,
                    )
                )
                self.update_completed_task_count(increment_by=1)


class WriteCSVExternalIds(XAPILakeCSVAsync):
    task_name = "Write CSV Actors"
    schema = "event_sink"
    table = "external_id"
    filename = "external_ids.csv.gz"

    async def _run_task(self):
        """
        Write out the user profile data and external id files.
        """
        async with self.get_csv_handle("external_ids") as external_id_csv_writer:
            actors = self.event_generator.actors
            self.update_total_task_count(len(actors))

            for actor in actors:
                dump_id = str(uuid.uuid4())
                dump_time = datetime.now(UTC)

                external_id_csv_writer.writerow(
                    (
                        actor.id,
                        "xapi",
                        actor.username,
                        actor.user_id,
                        dump_id,
                        dump_time,
                    )
                )
                self.update_completed_task_count(increment_by=1)


class WriteCSVProfiles(XAPILakeCSVAsync):
    task_name = "Write CSV Profiles"
    schema = "event_sink"
    table = "user_profile"
    filename = "user_profiles.csv.gz"

    async def _run_task(self):
        actors = self.event_generator.actors
        num_actor_profile_changes = self.config["num_actor_profile_changes"]
        self.update_total_task_count(len(actors) * num_actor_profile_changes)

        async with self.get_csv_handle("user_profiles") as profile_csv_writer:
            for i in range(num_actor_profile_changes):
                if i % 10 == 0:
                    self.logger.debug(f"   Actor save round {i}")
                    # This task can be very long running and prevent other tasks from
                    # running, so we sleep a bit to let other tasks run.
                    await asyncio.sleep(0.01)
                for actor in actors:
                    dump_id = str(uuid.uuid4())
                    dump_time = datetime.now(UTC)

                    profile_csv_writer.writerow(
                        (
                            # This first column is usually the MySQL row pk, we just
                            # user this for now to have a unique id.
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

                    self.update_completed_task_count(increment_by=1)
