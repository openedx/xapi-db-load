from datetime import datetime
import random

import clickhouse_connect


class XAPILakeClickhouse:
    def __init__(self, db_host="localhost", db_port=18123, db_username="default",
                 db_password=None, db_name=None):
        self.host = db_host
        self.port = db_port
        self.username = db_username
        self.database = db_name

        self.event_table_name = "xapi_events_all"

        self.client = clickhouse_connect.get_client(
            host=self.host,
            username=self.username,
            password=db_password,
            port=self.port,
            database=self.database,
            settings={
                'date_time_input_format': "best_effort",  # Allows RFC dates
                'allow_experimental_object_type': 1,  # Allows JSON type
            },
        )

    def print_db_time(self):
        res = self.client.query("SELECT timezone(), now()")
        print(res.result_set)

    def print_row_counts(self):
        print("Hard table row count:")
        res = self.client.query(f"SELECT count(*) FROM {self.event_table_name}")
        print(res.result_set)

    def create_db(self):
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")

    def drop_tables(self):
        self.client.command(f"DROP TABLE IF EXISTS {self.event_table_name}")
        print("Tables dropped")

    def create_tables(self):
        sql = f"""
            CREATE TABLE IF NOT EXISTS {self.event_table_name} (
                event_id UUID NOT NULL,
                verb_id String NOT NULL,
                actor_id UUID NOT NULL,
                org String NOT NULL,
                course_id String NOT NULL,
                emission_time timestamp NOT NULL,
                event JSON NOT NULL
            )
            ENGINE MergeTree ORDER BY (
                org, 
                course_id, 
                verb_id, 
                actor_id, 
                emission_time,
                event_id)
            PRIMARY KEY (org, course_id, verb_id, actor_id, emission_time, event_id)
            SETTINGS old_parts_lifetime=10
        """
        print(sql)
        self.client.command(sql)

        print("Table created")

    def batch_insert(self, events):
        """
        event_id UUID NOT NULL,
        verb_id String NOT NULL,
        actor_id UUID NOT NULL,
        org UUID NOT NULL,
        course_id String NULL,
        emission_time timestamp NOT NULL,
        event JSON NOT NULL
        """
        out_data = []
        for v in events:
            try:
                out = f"('{v['event_id']}', '{v['verb']}', '{v['actor_id']}', '{v['org']}', "
                out += f"'{v['course_run_id']}', " if "course_run_id" in v else "NULL, "
                out += f"'{v['emission_time']}', '{v['event']}')"
                out_data.append(out)
            except:
                print(v)
                raise
        vals = ",".join(out_data)

        # from pprint import pprint
        # pprint(out_data)

        self.client.command(
            f"""
            INSERT INTO {self.event_table_name} (
                event_id, 
                verb_id, 
                actor_id, 
                org,
                course_id, 
                emission_time, 
                event
            )
            VALUES {vals}
        """
        )

    def _run_query_and_print(self, query_name, query):
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
                where course_id = '{course_url}' 
                and verb_id = 'http://adlnet.gov/expapi/verbs/registered'
            """,
        )

        self._run_query_and_print(
            f"Count of total enrollment events for org {org}",
            f"""
                select count(*)
                from {self.event_table_name}
                where org = '{org}' 
                and verb_id = 'http://adlnet.gov/expapi/verbs/registered'
            """,
        )

        self._run_query_and_print(
            f"Count of enrollments for this learner",
            f"""
                select count(*)
                from {self.event_table_name}
                where actor_id = '{actor}' 
                and verb_id = 'http://adlnet.gov/expapi/verbs/registered'
            """,
        )

        self._run_query_and_print(
            f"Count of enrollments for this course - count of unenrollments, last 30 days",
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
            f"Count of enrollments for this course - count of unenrollments, all time",
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
            f"Count of enrollments for all courses - count of unenrollments, last 5 minutes",
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
        self._run_query_and_print(
           f"Count of courses",
           f"""
               select count(distinct course_id)
               from {self.event_table_name}
           """,
        )

        self._run_query_and_print(
           f"Count of learners",
           f"""
               select count(distinct actor_id)
               from {self.event_table_name}
           """,
        )

        self._run_query_and_print(
           f"Count of verbs",
           f"""
               select count(*), verb_id
               from {self.event_table_name}
               group by verb_id
           """,
        )

        self._run_query_and_print(
           f"Count of orgs",
           f"""
               select count(*), org
               from {self.event_table_name}
               group by org
           """,
        )

        self._run_query_and_print(
            f"Avg, min, max students per course",
            f"""
                select avg(a.num_students) as avg_students, min(a.num_students) as min_students, max(a.num_students) max_students
                from (
                    select count(distinct actor_id) as num_students
                    from {self.event_table_name}
                    group by course_id
                ) a
            """,
        )

        self._run_query_and_print(
           f"Avg, min, max problems per course",
           f"""
               select avg(a.num_problems) as avg_problems, min(a.num_problems) as min_problems,
                    max(a.num_problems) max_problems
                from (
                    select count(distinct event.object.id) as num_problems
                    from {self.event_table_name}
                    where event.object.definition.type = 'http://adlnet.gov/expapi/activities/cmi.interaction'
                    group by course_id
                ) a
           """,
        )

        self._run_query_and_print(
           f"Avg, min, max videos per course",
           f"""
               select avg(a.num_videos) as avg_videos, min(a.num_videos) as min_videos,
               max(a.num_videos) max_videos
               from (
                   select count(distinct event.object.id) as num_videos
                   from {self.event_table_name}
                   where event.object.definition.type = 'https://w3id.org/xapi/video/activity-type/video'
                   group by event.object.id
               ) a
           """,
        )

        self._run_query_and_print(
            f"Random event by id",
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
