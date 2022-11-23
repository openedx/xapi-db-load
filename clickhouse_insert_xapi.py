from datetime import datetime
import random

import clickhouse_connect


class XAPILake:
    def __init__(self, host, port, username, password=None, database=None):
        self.host = host
        self.port = port
        self.username = username
        self.database = database

        self.event_table_name = 'xapi_events_all'
        self.event_buffer_table_name = 'xapi_events_buffered_all'

        self.client = clickhouse_connect.get_client(
            host=self.host,
            username=self.username,
            password=password,
            port=self.port,
            database=self.database,
            date_time_input_format="best_effort",  # Allows RFC dates
            old_parts_lifetime=10  # Seconds, reduces disk usage
        )

    def print_db_time(self):
        res = self.client.query("SELECT timezone(), now()")
        print(res.result_set)

    def print_row_counts(self):
        res = self.client.query(f'SELECT count(*) FROM {self.event_buffer_table_name}')
        print("Buffer table row count:")
        print(res.result_set)

        print("Hard table row count:")
        res = self.client.query(f'SELECT count(*) FROM {self.event_table_name}')
        print(res.result_set)

    def create_db(self):
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")

    def drop_tables(self):
        self.client.command(f"DROP TABLE IF EXISTS {self.event_buffer_table_name}")
        self.client.command(f"DROP TABLE IF EXISTS {self.event_table_name}")
        print("Tables dropped")

    def create_tables(self):
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.event_table_name} (
            event_id UUID NOT NULL,
            verb String NOT NULL,
            actor_id UUID NOT NULL,
            org String NOT NULL,
            course_run_id String NOT NULL,
            problem_id String NULL,
            video_id String NULL,
            nav_starting_point String NULL,
            nav_ending_point String NULL,
            emission_time timestamp NOT NULL,
            event String NOT NULL
            )
            ENGINE MergeTree ORDER BY (course_run_id, verb, emission_time)
            PRIMARY KEY (course_run_id, verb)
        """)

        # Docs on buffer engine: https://clickhouse.com/docs/en/engines/table-engines/special/buffer/
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.event_buffer_table_name} AS {self.event_table_name} 
            ENGINE = Buffer(
                currentDatabase(), 
                {self.event_table_name}, 
                16, -- number of buffers to use, this is the recommended value
                10, -- minimum seconds between flushes (per buffer)
                100,  -- maximum seconds between flushes (per buffer)
                10000, -- minimum number of rows to flush (per buffer)
                1000000, -- maximum number of rows before flushing (per buffer)
                10000000, -- minimum number of bytes before flushing (per buffer)
                100000000 -- maximum number of bytes before flushing (per buffer)
            )
        """)

        print("Tables created")

    def batch_insert(self, events):
        """
            event_id UUID NOT NULL,
            verb String NOT NULL,
            actor_id UUID NOT NULL,
            org UUID NOT NULL,
            course_run_id String NULL,
            problem_id String NULL,
            video_id String NULL,
            nav_starting_point String NULL,
            nav_ending_point String NULL,
            emission_time timestamp NOT NULL,
            event String NOT NULL
        """
        out_data = []
        for v in events:
            try:
                out = f"('{v['event_id']}', '{v['verb']}', '{v['actor_id']}', '{v['org']}', "

                out += f"'{v['course_run_id']}', " if 'course_run_id' in v else "NULL, "
                out += f"'{v['problem_id']}', " if 'problem_id' in v else "NULL, "
                out += f"'{v['video_id']}', " if 'video_id' in v else "NULL, "
                out += f"'{v['nav_starting_point']}', " if 'nav_starting_point' in v else "NULL, "
                out += f"'{v['nav_ending_point']}', " if 'nav_ending_point' in v else "NULL, "

                out += f"'{v['emission_time']}', '{v['event']}')"
                out_data.append(out)
            except:
                print(v)
                raise
        vals = ",".join(out_data)

        # from pprint import pprint
        # pprint(out_data)

        self.client.command(f"""
            INSERT INTO {self.event_buffer_table_name} (
                event_id, 
                verb, 
                actor_id, 
                org,
                course_run_id, 
                problem_id, 
                video_id, 
                nav_starting_point, 
                nav_ending_point, 
                emission_time, 
                event
            )
            VALUES {vals}
        """)

    def _run_query_and_print(self, query_name, query):
        print(query_name)
        start_time = datetime.utcnow()
        result = self.client.query(query)
        end_time = datetime.utcnow()
        print(result.summary)
        print(result.result_set[:10])
        print("Completed in: " + str((end_time-start_time).total_seconds()))
        print("=================================")

    def do_queries(self, event_generator):
        """
        Query data from the table and document how long the query runs (while the insert script is running)
        :return:
        """
        # Get our randomly selected targets for this run
        course = random.choice(event_generator.known_courses)
        course_url = course.course_url
        org = random.choice(event_generator.known_orgs)
        actor = random.choice(event_generator.known_actor_uuids)

        self._run_query_and_print(
            f"Count of enrollment events for course {course_url}",
            f"""
                select count(*)
                from {self.event_table_name}
                where course_run_id = '{course_url}' 
                and verb = 'http://adlnet.gov/expapi/verbs/registered'
            """)

        self._run_query_and_print(
            f"Count of total enrollment events for org {org}",
            f"""
                select count(*)
                from {self.event_table_name}
                where org = '{org}' 
                and verb = 'http://adlnet.gov/expapi/verbs/registered'
            """)

        self._run_query_and_print(
            f"Count of enrollments for this learner",
            f"""
                select count(*)
                from {self.event_table_name}
                where actor_id = '{actor}' 
                and verb = 'http://adlnet.gov/expapi/verbs/registered'
            """)

        self._run_query_and_print(
            f"Count of enrollments for this course - count of unenrollments, last 30 days",
            f"""
                select a.cnt, b.cnt, a.cnt - b.cnt as total_registrations
                from (
                select count(*) cnt
                from {self.event_table_name}
                where course_run_id = '{course_url}'
                and verb = 'http://adlnet.gov/expapi/verbs/registered'
                and emission_time between date_sub(DAY, 30, now('UTC')) and now('UTC')) as a,
                (select count(*) cnt
                from {self.event_table_name}
                where course_run_id = '{course_url}'
                and verb = 'http://id.tincanapi.com/verb/unregistered'
                and emission_time between date_sub(DAY, 30, now('UTC')) and now('UTC')) as b
            """)

        # Number of enrollments for this course - number of unenrollments, all time
        self._run_query_and_print(
            f"Count of enrollments for this course - count of unenrollments, all time",
            f"""
                select a.cnt, b.cnt, a.cnt - b.cnt as total_registrations
                from (
                select count(*) cnt
                from {self.event_table_name}
                where course_run_id = '{course_url}'
                and verb = 'http://adlnet.gov/expapi/verbs/registered'
                ) as a,
                (select count(*) cnt
                from {self.event_table_name}
                where course_run_id = '{course.course_id}'
                and verb = 'http://id.tincanapi.com/verb/unregistered'
                ) as b
            """)

        self._run_query_and_print(
            f"Count of enrollments for all courses - count of unenrollments, last 5 minutes",
            f"""
                select a.cnt, b.cnt, a.cnt - b.cnt as total_registrations
                from (
                select count(*) cnt
                from {self.event_table_name}
                where verb = 'http://adlnet.gov/expapi/verbs/registered'
                and emission_time between date_sub(MINUTE, 5, now('UTC')) and now('UTC')) as a,
                (select count(*) cnt
                from {self.event_table_name}
                where verb = 'http://id.tincanapi.com/verb/unregistered'
                and emission_time between date_sub(MINUTE, 5, now('UTC')) and now('UTC')) as b
            """)
