from datetime import datetime
import random

from pymongo import MongoClient, IndexModel


class XAPILakeMongo:
    def __init__(self, host, port, username, password=None, database=None):
        self.host = host
        self.port = port
        self.username = username
        self.database = database

        self.event_collection_name = "xapi_events_all"

        # Provide the mongodb url to connect python to mongodb using pymongo
        # connection_string = f"mongodb://{username}:{password}@{host}/{database}"
        # For Tutor we're not using username / password
        connection_string = f"mongodb://{host}/{database}"
        print(connection_string)

        # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
        self.client = MongoClient(connection_string)

    def get_database(self):
        return self.client[self.database]

    def get_collection(self, create=False):
        if create:
            self.get_database().create_collection(self.event_collection_name)
        return self.get_database()[self.event_collection_name]

    def print_db_time(self):
        # This is a pain to do in mongo, not worth digging into for this
        print(datetime.utcnow())

    def print_row_counts(self):
        print("Collection count:")
        res = self.get_collection().count_documents({})
        print(res)

    def create_db(self):
        pass

    def drop_tables(self):
        self.get_collection().drop()
        print("Collection dropped")

    def create_tables(self):
        self.get_collection(create=True)
        # index1 = IndexModel(
        #    [
        #        ("org", ASCENDING),
        #        ("course_run_id", ASCENDING),
        #        ("verb", ASCENDING),
        #        ("actor_id", ASCENDING),
        #        ("emission_time", ASCENDING)
        #    ], name="full_field_index")
        indexes = [
            IndexModel("course_run_id"),
            IndexModel("org"),
            IndexModel("verb"),
            IndexModel("actor_id"),
            IndexModel("emission_time"),
        ]
        self.get_collection().create_indexes(indexes)

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
        for v in events:
            v["_id"] = v["event_id"]

        self.get_collection().insert_many(events)

    def _run_query_and_print(self, query_name, query_func, query_param):
        print(query_name)
        start_time = datetime.utcnow()
        result = query_func(query_param)
        end_time = datetime.utcnow()
        print(result)
        print("Completed in: " + str((end_time - start_time).total_seconds()))
        print("=================================")

    def _q_enrollments_for_course(self, course_url):
        return self.get_collection().count_documents(
            {
                "verb": "http://adlnet.gov/expapi/verbs/registered",
                "course_run_id": course_url,
            }
        )

    def _q_enrollments_for_org(self, org):
        return self.get_collection().count_documents(
            {"verb": "http://adlnet.gov/expapi/verbs/registered", "org": org}
        )

    def _q_enrollments_for_actor(self, actor):
        return self.get_collection().count_documents(
            {"verb": "http://adlnet.gov/expapi/verbs/registered", "actor": actor}
        )

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
            self._q_enrollments_for_course,
            course_url,
        )

        self._run_query_and_print(
            f"Count of total enrollment events for org {org}",
            self._q_enrollments_for_org,
            org,
        )

        self._run_query_and_print(
            f"Count of enrollments for this actor {actor}",
            self._q_enrollments_for_actor,
            actor,
        )

        # self._run_query_and_print(
        #    f"Count of enrollments for this course - count of unenrollments, last 30 days",
        #    f"""
        #        select a.cnt, b.cnt, a.cnt - b.cnt as total_registrations
        #        from (
        #        select count(*) cnt
        #        from {self.event_collection_name}
        #        where course_run_id = '{course_url}'
        #        and verb = 'http://adlnet.gov/expapi/verbs/registered'
        #        and emission_time between date_sub(DAY, 30, now('UTC')) and now('UTC')) as a,
        #        (select count(*) cnt
        #        from {self.event_collection_name}
        #        where course_run_id = '{course_url}'
        #        and verb = 'http://id.tincanapi.com/verb/unregistered'
        #        and emission_time between date_sub(DAY, 30, now('UTC')) and now('UTC')) as b
        #    """,
        # )

        # Number of enrollments for this course - number of unenrollments, all time
        # self._run_query_and_print(
        #    f"Count of enrollments for this course - count of unenrollments, all time",
        #    f"""
        #        select a.cnt, b.cnt, a.cnt - b.cnt as total_registrations
        #        from (
        #        select count(*) cnt
        #        from {self.event_collection_name}
        #        where course_run_id = '{course_url}'
        #        and verb = 'http://adlnet.gov/expapi/verbs/registered'
        #        ) as a,
        #        (select count(*) cnt
        #        from {self.event_collection_name}
        #        where course_run_id = '{course.course_id}'
        #        and verb = 'http://id.tincanapi.com/verb/unregistered'
        #        ) as b
        #    """,
        # )

        # self._run_query_and_print(
        #    f"Count of enrollments for all courses - count of unenrollments, last 5 minutes",
        #    f"""
        #        select a.cnt, b.cnt, a.cnt - b.cnt as total_registrations
        #        from (
        #        select count(*) cnt
        #        from {self.event_collection_name}
        #        where verb = 'http://adlnet.gov/expapi/verbs/registered'
        #        and emission_time between date_sub(MINUTE, 5, now('UTC')) and now('UTC')) as a,
        #        (select count(*) cnt
        #        from {self.event_collection_name}
        #        where verb = 'http://id.tincanapi.com/verb/unregistered'
        #        and emission_time between date_sub(MINUTE, 5, now('UTC')) and now('UTC')) as b
        #    """,
        # )
