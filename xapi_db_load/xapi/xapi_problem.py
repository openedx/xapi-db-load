from uuid import uuid4
import json

from .xapi_common import XAPIBase


# TODO: There are various other problem samples we should probably include eventually:
# https://github.com/openedx/event-routing-backends/tree/master/event_routing_backends/processors/xapi/tests/fixtures/expected
class BaseProblemCheck(XAPIBase):
    problem_type = None  # "browser" or "server"

    def get_data(self):
        event_id = str(uuid4())
        actor_id = self.parent_load_generator.get_actor()
        course = self.parent_load_generator.get_course()
        problem_id = course.get_problem_id()
        emission_time = course.get_random_emission_time()

        e = self.get_randomized_event(
            event_id, actor_id, course.course_url, problem_id, emission_time
        )

        return {
            "event_id": event_id,
            "verb": self.verb,
            "actor_id": actor_id,
            "org": course.org,
            "problem_id": problem_id,
            "course_run_id": course.course_url,
            "emission_time": emission_time,
            "event": e,
        }

    def get_randomized_event(
        self, event_id, account, course_locator, problem_id, create_time
    ):
        browser_object = {
            "object": {
                "definition": {
                    "type": "http://adlnet.gov/expapi/activities/cmi.interaction"
                },
                "id": problem_id,
                "objectType": "Activity",
            }
        }

        server_object = {
            "object": {
                "definition": {
                    "extensions": {"http://id.tincanapi.com/extension/attempt-id": 10},
                    "description": {
                        "en-US": "Add the question text, or prompt, here. This text is required."
                    },
                    "interactionType": "other",
                    "type": "http://adlnet.gov/expapi/activities/cmi.interaction",
                },
                "id": problem_id,
                "objectType": "Activity",
            },
            "result": {
                "response": "A correct answer",
                "score": {"max": 1, "min": 0, "raw": 0, "scaled": 0},
                "success": False,
            },
        }

        event = {
            "id": event_id,
            "actor": {
                "objectType": "Agent",
                "account": {"homePage": "http://localhost:18000", "name": account},
            },
            "context": {
                "contextActivities": {
                    "parent": [
                        {
                            "id": course_locator,
                            "objectType": "Activity",
                            "definition": {
                                "name": {"en-US": "Demonstration Course"},
                                "type": "http://adlnet.gov/expapi/activities/course",
                            },
                        }
                    ]
                },
                "extensions": {
                    "https://github.com/openedx/event-routing-backends/blob/master/docs/xapi-extensions/eventVersion.rst": "1.0"
                },
            },
            "timestamp": create_time.isoformat(),
            "verb": {"display": {"en": self.verb_display}, "id": self.verb},
            "version": "1.0.3",
        }

        if self.problem_type == "browser":
            event.update(browser_object)
        else:
            event.update(server_object)

        return json.dumps(event)


class BrowserProblemCheck(BaseProblemCheck):
    verb = "http://adlnet.gov/expapi/verbs/attempted"
    verb_display = "attempted"
    problem_type = "browser"


class ServerProblemCheck(BaseProblemCheck):
    verb = "http://adlnet.gov/expapi/verbs/evaluated"
    verb_display = "evaluated"
    problem_type = "server"
