from datetime import datetime
import random

from pymongo import MongoClient, IndexModel, ASCENDING


class XAPILakeMongo:
    def __init__(self, db_host, db_port, db_username=None, db_password=None,
                 db_name="statements"):
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
        return
        self.get_collection(create=True)

        indexes = [
            IndexModel([("course_run_id", ASCENDING), ("verb", ASCENDING)], name="course_verb"),
            IndexModel("org"),
            IndexModel("actor_id"),
            IndexModel("emission_time"),
        ]

        self.get_collection().create_indexes(indexes)

    def batch_insert(self, events):
        for v in events:
            v["_id"] = v["event_id"]

        self.get_collection().insert_many(events)

    def _run_query_and_print(self, query_name, query_func, query_param=None):
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
        return self.get_collection().count_documents({"$and":
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
        })

    def _q_enrollments_for_org(self, org):
        return self.get_collection().count_documents(
            {"_source.verb.id": "http://adlnet.gov/expapi/verbs/registered", "_source.org.id": org}
        )

    def _q_enrollments_for_actor(self, actor):
        return self.get_collection().count_documents({
            "_source.verb.id": "http://adlnet.gov/expapi/verbs/registered",
            "_source.actor.account.name": actor
        })

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

    # Distribution queries
    def _q_count_courses(self):
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
        with self.get_collection().aggregate([
            {"$group": {"_id": "$_source.actor.account.name"}},
            {"$count": "ActorCount"}
        ]) as cursor:
            for x in cursor:
                return x["ActorCount"]

    def _q_count_verb_dist(self):
        out = []
        for x in self.get_collection().aggregate([
            {"$group": {"_id": "$_source.verb.id", "count": {"$sum": 1}}},
        ]):
            out.append(x)
        return out

    def _q_count_org_dist(self):
        pass

    def do_distributions(self):
        self._run_query_and_print(
            f"Count of courses",
            self._q_count_courses,
        )

        self._run_query_and_print(
            f"Count of learners",
            self._q_count_learners,
        )

        self._run_query_and_print(
            f"Count of verbs",
            self._q_count_verb_dist,
        )

        self._run_query_and_print(
            f"Count of orgs",
            self._q_count_org_dist
        )
