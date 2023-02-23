"""
Mongo backend implementation.
"""

import random
from datetime import datetime

from pymongo import ASCENDING, IndexModel, MongoClient


class XAPILakeMongo:
    """
    Mongo backend, controls the collections, insertion, and queries for our random events.
    """

    def __init__(self, db_host, db_port, db_username=None, db_password=None,
                 db_name="statements"):
        """
        Init the backend.
        """
        self.host = db_host
        self.port = db_port
        self.username = db_username
        self.password = db_password
        self.database = db_name
        self.event_collection_name = "foo"  # This is what Ralph currently calls it

        # Provide the mongodb url to connect python to mongodb using pymongo
        # connection_string = f"mongodb://{username}:{password}@{host}/{database}"
        if db_username:
            connection_string = f"mongodb://{db_username}:" \
                                f"{db_password}@{db_host}/{db_name}"
        else:
            # For Tutor we're not using username / password
            connection_string = f"mongodb://{db_host}/{db_name}"

        # Create a connection using MongoClient. You can import MongoClient or use
        # pymongo.MongoClient
        self.client = MongoClient(connection_string)

    def get_database(self):
        """
        Return the Mongo database.
        """
        return self.client[self.database]

    def get_collection(self, create=False):
        """
        Return the Mongo event collection, optionally create it.
        """
        if create:
            self.get_database().create_collection(self.event_collection_name)
        return self.get_database()[self.event_collection_name]

    def print_db_time(self):
        """
        Print the current time.
        """
        # This is a pain to do in mongo, not worth digging into for this
        print(datetime.utcnow())

    def print_row_counts(self):
        """
        Print the count of documents in the event collection.
        """
        print("Collection count:")
        res = self.get_collection().count_documents({})
        print(res)

    def create_db(self):
        """
        Create the database.

        No need to create a database in Mongo.
        """

    def drop_tables(self):
        """
        Drop the collection.
        """
        self.get_collection().drop()
        print("Collection dropped")

    def create_tables(self):
        """
        Create a collection and indexes for our test.
        """
        self.get_collection(create=True)

        # So far these are the best performing indexes, but I suspect there is room for improvement
        indexes = [
            IndexModel([("course_run_id", ASCENDING), ("verb", ASCENDING)], name="course_verb"),
            IndexModel("org"),
            IndexModel("actor_id"),
            IndexModel("emission_time"),
        ]

        self.get_collection().create_indexes(indexes)

    def batch_insert(self, events):
        """
        Insert a batch of Mongo documents.
        """
        for v in events:
            v["_id"] = v["event_id"]

        self.get_collection().insert_many(events)

    def _run_query_and_print(self, query_name, query_func, query_param=None):
        """
        Run the given query function and prints timing data for the logs.
        """
        print(query_name)
        start_time = datetime.utcnow()
        if query_param:
            result = query_func(query_param)
        else:
            result = query_func()
        end_time = datetime.utcnow()
        print(result)
        print("Completed in: " + str((end_time - start_time).total_seconds()))
        print("=================================")

    # Queries during load
    def _q_enrollments_for_course(self, course_url):
        """
        Return the count of documents for a course.
        """
        return self.get_collection().count_documents(
            {
                "$and":
                [
                    {"_source.verb.id": "http://adlnet.gov/expapi/verbs/registered"},
                    {"$and": [
                        {
                            "_source.object.definition.type": "http://adlnet.gov/expapi/activities/course"
                        },
                        {
                            "_source.object.id": course_url
                        },
                    ]},
                ]
            }
        )

    def _q_enrollments_for_org(self, org):
        """
        Return the count of documents for an org.
        """
        return self.get_collection().count_documents(
            {"_source.verb.id": "http://adlnet.gov/expapi/verbs/registered", "_source.org.id": org}
        )

    def _q_enrollments_for_actor(self, actor):
        """
        Return the count of documents for an actor.
        """
        return self.get_collection().count_documents({
            "_source.verb.id": "http://adlnet.gov/expapi/verbs/registered",
            "_source.actor.account.name": actor
        })

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

    # Distribution queries
    def _q_count_courses(self):
        """
        Return a count of registrations for a course.
        """
        with self.get_collection().aggregate([
            {"$match": {
                "_source.verb.id": "http://adlnet.gov/expapi/verbs/registered",
            }},
            {"$group": {"_id": "$_source.object.id"}},
            {"$count": "CourseCount"}
        ]) as cursor:
            for x in cursor:
                return x["CourseCount"]

    def _q_count_learners(self):
        """
        Return the count of all distinct learners.
        """
        with self.get_collection().aggregate([
            {"$group": {"_id": "$_source.actor.account.name"}},
            {"$count": "ActorCount"}
        ]) as cursor:
            for x in cursor:
                return x["ActorCount"]

    def _q_count_verb_dist(self):
        """
        Return the count of events aggregated by verb.
        """
        out = []
        for x in self.get_collection().aggregate([
            {"$group": {"_id": "$_source.verb.id", "count": {"$sum": 1}}},
        ]):
            out.append(x)
        return out

    def _q_count_org_dist(self):
        """
        Return the count of events for all orgs.

        This was not actually done before we decided against Mongo.
        """

    def do_distributions(self):
        """
        Run and print the results of the various distribution queries.
        """
        self._run_query_and_print(
            "Count of courses",
            self._q_count_courses,
        )

        self._run_query_and_print(
            "Count of learners",
            self._q_count_learners,
        )

        self._run_query_and_print(
            "Count of verbs",
            self._q_count_verb_dist,
        )

        self._run_query_and_print(
            "Count of orgs",
            self._q_count_org_dist
        )
