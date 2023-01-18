from datetime import datetime
import random

import psycopg2


class XAPILakeCitus:
    def __init__(self, db_host, db_port, db_username, db_password=None,
                 db_name=None):
        self.host = db_host
        self.port = db_port
        self.username = db_username
        self.database = db_name

        self.event_table_name = "xapi_events_all"

        self.client = psycopg2.connect(
            host=self.host,
            user=self.username,
            password=db_password,
            port=self.port,
            database=self.database,
        )

        self.cursor = self.client.cursor()

    def print_db_time(self):
        res = self.cursor.execute("SELECT current_setting('TIMEZONE'), now()")
        print(self.cursor.fetchone())

    def print_row_counts(self):
        print("Table row count:")
        res = self.cursor.execute(f"SELECT count(*) FROM {self.event_table_name}")
        print(f"{self.cursor.fetchone()[0]:,}")

    def create_db(self):
        self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")

    def commit(self):
        self.client.commit()

    def drop_tables(self):
        self.cursor.execute(f"DROP TABLE IF EXISTS {self.event_table_name}")
        self.commit()
        print("Tables dropped")

    def create_tables(self):
        sql = f"""
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
            ) 
            PARTITION BY RANGE (emission_time);
        """

        print(sql)
        self.cursor.execute(sql)
        self.commit()

        # Citus wants the distribution key and partition key in the primary key or will otherwise ignore them
        sql = f"""
            ALTER TABLE {self.event_table_name}
              ADD CONSTRAINT {self.event_table_name}_pkey
              PRIMARY KEY (course_run_id, event_id, emission_time);
            """
        print(sql)
        self.cursor.execute(sql)
        self.commit()

        sql = f"""
                SELECT create_time_partitions(
                  table_name         := '{self.event_table_name}',
                  partition_interval := '1 month',
                  start_from         := now() - interval '6 years',
                  end_at             := now() + interval '1 months'
                );
        """
        print(sql)
        self.cursor.execute(sql)
        self.commit()

        sql = f"""
            CREATE INDEX course_verb ON xapi_events_all (course_run_id, verb);
            CREATE INDEX org ON xapi_events_all (org);
            CREATE INDEX actor ON xapi_events_all (actor_id);
            """
        print(sql)
        self.cursor.execute(sql)
        self.commit()

        # This tells Citus to distribute the table based on course run, keeping all of those events together
        sql = f"SELECT create_distributed_table('{self.event_table_name}', 'course_run_id')"
        print(sql)
        self.cursor.execute(sql)
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

    def do_distributions(self):
        self._run_query_and_print(
           f"Count of courses",
           f"""
               select count(distinct course_run_id)
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
               select count(*), verb
               from {self.event_table_name}
               group by verb
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
                    group by course_run_id
                ) a
            """,
        )

        self._run_query_and_print(
            f"Avg, min, max problems per course",
            f"""
                select avg(a.num_problems) as avg_problems, min(a.num_problems) as min_problems, max(a.num_problems) max_problems
                from (
                    select count(distinct problem_id) as num_problems
                    from {self.event_table_name}
                    group by course_run_id
                ) a
            """,
        )

        self._run_query_and_print(
            f"Avg, min, max videos per course",
            f"""
                select avg(a.num_videos) as avg_videos, min(a.num_videos) as min_videos, max(a.num_videos) max_videos
                from (
                    select count(distinct problem_id) as num_videos
                    from {self.event_table_name}
                    group by video_id
                ) a
            """,
        )

        self._run_query_and_print(
            f"Random event by id",
            f"""
                select actor_id
                from {self.event_table_name} 
                where event_id = (
                    select event_id
                    from {self.event_table_name}
                    limit 1    
                )
            """,
        )
