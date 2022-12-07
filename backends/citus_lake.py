from datetime import datetime
import random

import psycopg2


class XAPILakeCitus:
    def __init__(self, host, port, username, password=None, database=None):
        self.host = host
        self.port = port
        self.username = username
        self.database = database

        self.event_table_name = "xapi_events_all"

        self.client = psycopg2.connect(
            host=self.host,
            user=self.username,
            password=password,
            port=self.port,
            database=self.database,
        )

        self.cursor = self.client.cursor()

    def print_db_time(self):
        res = self.cursor.execute("SELECT current_setting('TIMEZONE'), now()")
        print(self.cursor.fetchone())

    def print_row_counts(self):
        print("Hard table row count:")
        res = self.cursor.execute(f"SELECT count(*) FROM {self.event_table_name}")
        print(self.cursor.fetchone())

    def create_db(self):
        self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")

    def commit(self):
        self.client.commit()

    def drop_tables(self):
        self.cursor.execute(f"DROP TABLE IF EXISTS {self.event_table_name}")
        self.commit()
        print("Tables dropped")

    def create_tables(self):
        """
        CREATE TABLE github_events
(
    event_id bigint,
    event_type text,
    event_public boolean,
    repo_id bigint,
    payload jsonb,
    repo jsonb,
    actor jsonb,
    org jsonb,
    created_at timestamp
);
        :return:
        """
        self.cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.event_table_name} (
            event_id uuid NOT NULL,
            verb text NOT NULL,
            actor_id uuid NOT NULL,
            org text NOT NULL,
            course_run_id text NOT NULL,
            problem_id text NULL,
            video_id text NULL,
            nav_starting_point text NULL,
            nav_ending_point text NULL,
            emission_time timestamp NOT NULL,
            event jsonb NOT NULL
            ) USING columnar
        """
        )

        self.commit()

        self.cursor.execute(
            f"""
            CREATE INDEX course_verb ON xapi_events_all (course_run_id, verb);
            CREATE INDEX org ON xapi_events_all (org);
            CREATE INDEX actor ON xapi_events_all (actor_id);
            """
        )

        # This tells Citus to distribute the table based on course run, keeping all of those events together
        self.cursor.execute(f"SELECT create_distributed_table('{self.event_table_name}', 'course_run_id')")

        self.commit()

        print("Table created")

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

                out += f"'{v['course_run_id']}', " if "course_run_id" in v else "NULL, "
                out += f"'{v['problem_id']}', " if "problem_id" in v else "NULL, "
                out += f"'{v['video_id']}', " if "video_id" in v else "NULL, "
                out += (
                    f"'{v['nav_starting_point']}', "
                    if "nav_starting_point" in v
                    else "NULL, "
                )
                out += (
                    f"'{v['nav_ending_point']}', "
                    if "nav_ending_point" in v
                    else "NULL, "
                )

                out += f"'{v['emission_time']}', '{v['event']}')"
                out_data.append(out)
            except:
                print(v)
                raise
        vals = ",".join(out_data)

        # from pprint import pprint
        # pprint(out_data)

        self.cursor.execute(
            f"""
            INSERT INTO {self.event_table_name} (
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
        """
        )

        self.commit()

    def _run_query_and_print(self, query_name, query):
        print(query_name)
        # print(query)
        start_time = datetime.utcnow()
        self.cursor.execute(query)
        end_time = datetime.utcnow()
        print(self.cursor.fetchmany(size=20))
        print("Completed in: " + str((end_time - start_time).total_seconds()))
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
            """,
        )

        self._run_query_and_print(
            f"Count of total enrollment events for org {org}",
            f"""
                select count(*)
                from {self.event_table_name}
                where org = '{org}' 
                and verb = 'http://adlnet.gov/expapi/verbs/registered'
            """,
        )

        self._run_query_and_print(
            f"Count of enrollments for this learner",
            f"""
                select count(*)
                from {self.event_table_name}
                where actor_id = '{actor}' 
                and verb = 'http://adlnet.gov/expapi/verbs/registered'
            """,
        )

        self._run_query_and_print(
            f"Count of enrollments for this course - count of unenrollments, last 30 days",
            f"""
                select a.cnt, b.cnt, a.cnt - b.cnt as total_registrations
                from (
                select count(*) cnt
                from {self.event_table_name}
                where course_run_id = '{course_url}'
                and verb = 'http://adlnet.gov/expapi/verbs/registered'
                and emission_time between now() - interval '30 day' and now()) as a,
                (select count(*) cnt
                from {self.event_table_name}
                where course_run_id = '{course_url}'
                and verb = 'http://id.tincanapi.com/verb/unregistered'
                and emission_time between now() - interval '30 day' and now()) as b
            """,
        )

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
            """,
        )

        self._run_query_and_print(
            f"Count of enrollments for all courses - count of unenrollments, last 5 minutes",
            f"""
                select a.cnt, b.cnt, a.cnt - b.cnt as total_registrations
                from (
                select count(*) cnt
                from {self.event_table_name}
                where verb = 'http://adlnet.gov/expapi/verbs/registered'
                and emission_time between now() - interval '5 minute' and now()) as a,
                (select count(*) cnt
                from {self.event_table_name}
                where verb = 'http://id.tincanapi.com/verb/unregistered'
                and emission_time between now() - interval '5 minute' and now()) as b
            """,
        )
