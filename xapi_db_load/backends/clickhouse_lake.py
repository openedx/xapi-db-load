"""
ClickHouse data lake implementation.
"""

import random
import uuid
from datetime import datetime

import clickhouse_connect


class XAPILakeClickhouse:
    """
    Lake implementation for ClickHouse.
    """

    client = None

    def __init__(
        self,
        db_host="localhost",
        db_port=18123,
        db_username="default",
        db_password=None,
        db_name=None,
    ):
        self.host = db_host
        self.port = db_port
        self.username = db_username
        self.database = db_name
        self.db_password = db_password

        self.event_raw_table_name = "xapi_events_all"
        self.event_table_name = "xapi_events_all_parsed"
        self.event_table_name_mv = "xapi_events_all_parsed_mv"
        self.get_org_function_name = "get_org_from_course_url"
        self.set_client()

    def set_client(self):
        """
        Set up the ClickHouse client and connect.
        """
        client_options = {
            "date_time_input_format": "best_effort",  # Allows RFC dates
            "allow_experimental_object_type": 1,  # Allows JSON data type
        }

        # For some reason get_client isn't automatically setting secure based on the port
        # so we have to do it ourselves. This is obviously limiting, but should be 90% correct
        # and keeps us from adding yet another command line option.
        secure = str(self.port).endswith("443") or str(self.port).endswith("440")

        self.client = clickhouse_connect.get_client(
            host=self.host,
            username=self.username,
            password=self.db_password,
            port=self.port,
            database=self.database,
            settings=client_options,
            secure=secure,
        )

    def print_db_time(self):
        """
        Print the current time according to the db.
        """
        res = self.client.query("SELECT timezone(), now()")
        # Always flush our output on these so we can follow the logs.
        print(res.result_set, flush=True)

    def print_row_counts(self):
        """
        Print the current row count.
        """
        print("Hard table row count:")
        res = self.client.query(f"SELECT count(*) FROM {self.event_table_name}")
        print(res.result_set)

    def create_db(self):
        """
        Create the destination database if it doesn't exist.
        """
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")

    def drop_tables(self):
        """
        Drop existing table structures.
        """
        self.client.command(f"DROP TABLE IF EXISTS {self.event_raw_table_name}")
        self.client.command(f"DROP TABLE IF EXISTS {self.event_table_name}")
        self.client.command(f"DROP FUNCTION IF EXISTS {self.get_org_function_name}")
        self.client.command(f"DROP TABLE IF EXISTS {self.event_table_name_mv}")
        print("Tables dropped")

    def create_tables(self):
        """
        Create the base xAPI tables and top level materialized views.

        In the future we should manage this through the scripts in tutor-contrib-aspects to keep the
        table structures compatible.
        """
        sql = f"""
            CREATE TABLE IF NOT EXISTS {self.event_raw_table_name} (
                event_id UUID NOT NULL,
                emission_time DateTime64(6) NOT NULL,
                event JSON NOT NULL,
                event_str String NOT NULL,
            )
            ENGINE MergeTree ORDER BY (
                emission_time,
                event_id)
            PRIMARY KEY (emission_time, event_id)
        """

        print(sql)
        self.client.command(sql)

        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.event_table_name} (
            event_id UUID NOT NULL,
            verb_id String NOT NULL,
            actor_id String NOT NULL,
            object_id String NOT NULL,
            org String NOT NULL,
            course_id String NOT NULL,
            emission_time DateTime64(6) NOT NULL,
            event_str String NOT NULL
        ) ENGINE MergeTree
        ORDER BY (org, course_id, verb_id, actor_id, emission_time, event_id)
        PRIMARY KEY (org, course_id, verb_id, actor_id, emission_time, event_id);
        """

        print(sql)
        self.client.command(sql)

        sql = f"""
        CREATE OR REPLACE FUNCTION {self.get_org_function_name} AS (course_url) ->
        nullIf(EXTRACT(course_url, 'course-v1:([a-zA-Z0-9]*)'), '')
        ;"""

        print(sql)
        self.client.command(sql)

        sql = f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {self.event_table_name_mv}
            TO {self.event_table_name} AS
            SELECT
            event_id as event_id,
            JSON_VALUE(event_str, '$.verb.id') as verb_id,
            JSON_VALUE(event_str, '$.actor.account.name') as actor_id,
            JSON_VALUE(event_str, '$.object.id') as object_id,
            -- If the contextActivities parent is a course, use that. Otherwise use the object id for the course id
            if(
                JSON_VALUE(
                    event_str,
                    '$.context.contextActivities.parent[0].definition.type')
                        = 'http://adlnet.gov/expapi/activities/course',
                    JSON_VALUE(event_str, '$.context.contextActivities.parent[0].id'),
                    JSON_VALUE(event_str, '$.object.id')
                ) as course_id,
            {self.get_org_function_name}(course_id) as org,
            emission_time as emission_time,
            event_str as event_str
            FROM {self.event_raw_table_name};
        """
        print(sql)
        self.client.command(sql)

        sql = """
            CREATE TABLE IF NOT EXISTS event_sink.course_overviews
            (
                org              String,
                course_key       String,
                display_name     String,
                course_start     String,
                course_end       String,
                enrollment_start String,
                enrollment_end   String,
                self_paced       Bool,
                course_data_json String,
                created          String,
                modified         String,
                dump_id          UUID,
                time_last_dumped String
            )
                engine = MergeTree PRIMARY KEY (org, course_key, modified, time_last_dumped)
                    ORDER BY (org, course_key, modified, time_last_dumped);
        """

        print(sql)
        self.client.command(sql)

        sql = """
            CREATE TABLE IF NOT EXISTS event_sink.course_blocks
            (
                org              String,
                course_key       String,
                location         String,
                display_name     String,
                xblock_data_json String,
                order            Int32 default 0,
                edited_on        String,
                dump_id          UUID,
                time_last_dumped String
            )
                engine = MergeTree PRIMARY KEY (org, course_key, location, edited_on)
                    ORDER BY (org, course_key, location, edited_on, order);
        """
        print(sql)
        self.client.command(sql)

        print("Tables created")

    def batch_insert(self, events):
        """
        Insert a batch of events to ClickHouse.
        """
        out_data = []
        for v in events:
            try:
                out = f"('{v['event_id']}', '{v['emission_time']}', '{v['event']}', '{v['event']}')"
                out_data.append(out)
            except Exception:
                print(v)
                raise
        vals = ",".join(out_data)
        sql = f"""
                INSERT INTO {self.event_raw_table_name} (
                    event_id,
                    emission_time,
                    event,
                    event_str
                )
                VALUES {vals}
            """
        # Sometimes the connection randomly dies, this gives us a second shot in that case
        try:
            self.client.command(sql)
        except clickhouse_connect.driver.exceptions.OperationalError:
            print("ClickHouse OperationalError, trying to reconnect.")
            self.set_client()
            print("Retrying insert...")
            self.client.command(sql)

    def insert_event_sink_course_data(self, courses):
        """
        Insert the course overview data to ClickHouse.

        This allows us to test join performance to get course and block names.
        """
        out_data = []
        for course in courses:
            c = course.serialize_course_data_for_event_sink()
            dump_id = str(uuid.uuid4())
            dump_time = datetime.utcnow()
            try:
                out = f"""(
                    '{c['org']}',
                    '{c['course_key']}',
                    '{c['display_name']}',
                    '{c['course_start']}',
                    '{c['course_end']}',
                    '{c['enrollment_start']}',
                    '{c['enrollment_end']}',
                    '{c['self_paced']}',
                    '{c['course_data_json']}',
                    '{c['created']}',
                    '{c['modified']}',
                    '{dump_id}',
                    '{dump_time}'
                )"""
                out_data.append(out)
            except Exception:
                print(c)
                raise
        vals = ",".join(out_data)
        sql = f"""
                INSERT INTO event_sink.course_overviews (
                    org,
                    course_key,
                    display_name,
                    course_start,
                    course_end,
                    enrollment_start,
                    enrollment_end,
                    self_paced,
                    course_data_json,
                    created,
                    modified,
                    dump_id,
                    time_last_dumped
                )
                VALUES {vals}
            """
        # Sometimes the connection randomly dies, this gives us a second shot in that case
        try:
            self.client.command(sql)
        except clickhouse_connect.driver.exceptions.OperationalError:
            print("ClickHouse OperationalError, trying to reconnect.")
            self.set_client()
            print("Retrying insert...")
            self.client.command(sql)

    def insert_event_sink_block_data(self, courses):
        """
        Insert the block data to ClickHouse.

        This allows us to test join performance to get course and block names.
        """
        for course in courses:
            out_data = []
            blocks = course.serialize_block_data_for_event_sink()
            dump_id = str(uuid.uuid4())
            dump_time = datetime.utcnow()
            for b in blocks:
                try:
                    out = f"""(
                        '{b['org']}',
                        '{b['course_key']}',
                        '{b['location']}',
                        '{b['display_name']}',
                        '{b['xblock_data_json']}',
                        '{b['order']}',
                        '{b['edited_on']}',
                        '{dump_id}',
                        '{dump_time}'
                    )"""
                    out_data.append(out)
                except Exception:
                    print(b)
                    raise

            vals = ",".join(out_data)
            sql = f"""
                    INSERT INTO event_sink.course_blocks (
                        org,
                        course_key,
                        location,
                        display_name,
                        xblock_data_json,
                        order,
                        edited_on,
                        dump_id,
                        time_last_dumped
                    )
                    VALUES {vals}
                """
            # Sometimes the connection randomly dies, this gives us a second shot in that case
            try:
                self.client.command(sql)
            except clickhouse_connect.driver.exceptions.OperationalError:
                print("ClickHouse OperationalError, trying to reconnect.")
                self.set_client()
                print("Retrying insert...")
                self.client.command(sql)

    def _run_query_and_print(self, query_name, query):
        """
        Execute a ClickHouse query and print the elapsed client time.
        """
        print(query_name)
        start_time = datetime.utcnow()
        result = self.client.query(query)
        end_time = datetime.utcnow()
        print(result.summary)
        print(result.result_set[:10])
        print("Completed in: " + str((end_time - start_time).total_seconds()))
        print("=================================")

    def do_queries(self, event_generator):
        """
        Query data from the table and document how long the query runs (while the insert script is running).
        """
        # Get our randomly selected targets for this run
        course = random.choice(event_generator.known_courses)
        course_url = course.course_url
        org = random.choice(event_generator.known_orgs)
        actor = random.choice(event_generator.known_actor_uuids)

        self._run_query_and_print(
            "Count of enrollment events for course {course_url}",
            f"""
                select count(*)
                from {self.event_table_name}
                where course_id = '{course_url}'
                and verb_id = 'http://adlnet.gov/expapi/verbs/registered'
            """,
        )

        self._run_query_and_print(
            "Count of total enrollment events for org {org}",
            f"""
                select count(*)
                from {self.event_table_name}
                where org = '{org}'
                and verb_id = 'http://adlnet.gov/expapi/verbs/registered'
            """,
        )

        self._run_query_and_print(
            "Count of enrollments for this learner",
            f"""
                select count(*)
                from {self.event_table_name}
                where actor_id = '{actor}'
                and verb_id = 'http://adlnet.gov/expapi/verbs/registered'
            """,
        )

        self._run_query_and_print(
            "Count of enrollments for this course - count of unenrollments, last 30 days",
            f"""
                select a.cnt, b.cnt, a.cnt - b.cnt as total_registrations
                from (
                select count(*) cnt
                from {self.event_table_name}
                where course_id = '{course_url}'
                and verb_id = 'http://adlnet.gov/expapi/verbs/registered'
                and emission_time between date_sub(DAY, 30, now('UTC')) and now('UTC')) as a,
                (select count(*) cnt
                from {self.event_table_name}
                where course_id = '{course_url}'
                and verb_id = 'http://id.tincanapi.com/verb/unregistered'
                and emission_time between date_sub(DAY, 30, now('UTC')) and now('UTC')) as b
            """,
        )

        # Number of enrollments for this course - number of unenrollments, all time
        self._run_query_and_print(
            "Count of enrollments for this course - count of unenrollments, all time",
            f"""
                select a.cnt, b.cnt, a.cnt - b.cnt as total_registrations
                from (
                select count(*) cnt
                from {self.event_table_name}
                where course_id = '{course_url}'
                and verb_id = 'http://adlnet.gov/expapi/verbs/registered'
                ) as a,
                (select count(*) cnt
                from {self.event_table_name}
                where course_id = '{course.course_id}'
                and verb_id = 'http://id.tincanapi.com/verb/unregistered'
                ) as b
            """,
        )

        self._run_query_and_print(
            "Count of enrollments for all courses - count of unenrollments, last 5 minutes",
            f"""
                select a.cnt, b.cnt, a.cnt - b.cnt as total_registrations
                from (
                select count(*) cnt
                from {self.event_table_name}
                where verb_id = 'http://adlnet.gov/expapi/verbs/registered'
                and emission_time between date_sub(MINUTE, 5, now('UTC')) and now('UTC')) as a,
                (select count(*) cnt
                from {self.event_table_name}
                where verb_id = 'http://id.tincanapi.com/verb/unregistered'
                and emission_time between date_sub(MINUTE, 5, now('UTC')) and now('UTC')) as b
            """,
        )

    def do_distributions(self):
        """
        Execute and print the timing of distribution queries to enable comparisons across runs.
        """
        self._run_query_and_print(
            "Count of courses",
            f"""
               select count(distinct course_id)
               from {self.event_table_name}
           """,
        )

        self._run_query_and_print(
            "Count of learners",
            f"""
               select count(distinct actor_id)
               from {self.event_table_name}
           """,
        )

        self._run_query_and_print(
            "Count of verbs",
            f"""
               select count(*), verb_id
               from {self.event_table_name}
               group by verb_id
           """,
        )

        self._run_query_and_print(
            "Count of orgs",
            f"""
               select count(*), org
               from {self.event_table_name}
               group by org
           """,
        )

        self._run_query_and_print(
            "Avg, min, max students per course",
            f"""
                select avg(a.num_students) as avg_students,
                        min(a.num_students) as min_students,
                        max(a.num_students) max_students
                from (
                    select count(distinct actor_id) as num_students
                    from {self.event_table_name}
                    group by course_id
                ) a
            """,
        )

        self._run_query_and_print(
            "Avg, min, max problems per course",
            f"""
               select avg(a.num_problems) as avg_problems, min(a.num_problems) as min_problems,
                    max(a.num_problems) max_problems
                from (
                    select count(distinct object_id) as num_problems
                    from {self.event_table_name}
                    where JSON_VALUE(event_str, '$.object.definition.type') =
                    'http://adlnet.gov/expapi/activities/cmi.interaction'
                    group by course_id
                ) a
           """,
        )

        self._run_query_and_print(
            "Avg, min, max videos per course",
            f"""
               select avg(a.num_videos) as avg_videos, min(a.num_videos) as min_videos,
               max(a.num_videos) max_videos
               from (
                   select count(distinct object_id) as num_videos
                   from {self.event_table_name}
                   where JSON_VALUE(event_str, '$.object.definition.type') =
                    'https://w3id.org/xapi/video/activity-type/video'
                   group by object_id
               ) a
           """,
        )

        self._run_query_and_print(
            "Random event by id",
            f"""
                select *
                from {self.event_table_name}
                where event_id = (
                    select event_id
                    from {self.event_table_name}
                    limit 1
                )
            """,
        )
