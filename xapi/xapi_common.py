from random import randrange
import datetime

END_DATETIME = datetime.datetime.utcnow()
START_DATETIME = END_DATETIME - datetime.timedelta(days=365*5)


def random_date():
    delta = END_DATETIME - START_DATETIME
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return START_DATETIME + datetime.timedelta(seconds=random_second)


class XAPIBase:
    verb = None
    verb_display = None

    def __init__(self, load_generator):
        if not self.verb:
            raise NotImplementedError(f"XAPIBase is abstract, add your verb in subclass {type(self)}.")
        self.parent_load_generator = load_generator
