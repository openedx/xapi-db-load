from random import randrange
import datetime


def random_date():
    end_datetime = datetime.datetime.utcnow()
    start_datetime = end_datetime - datetime.timedelta(days=365 * 5)
    delta = end_datetime - start_datetime
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start_datetime + datetime.timedelta(seconds=random_second)


class XAPIBase:
    verb = None
    verb_display = None

    def __init__(self, load_generator):
        if not self.verb:
            raise NotImplementedError(f"XAPIBase is abstract, add your verb in subclass {type(self)}.")
        self.parent_load_generator = load_generator
