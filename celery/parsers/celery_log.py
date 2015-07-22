""" Datadog log parser for celery.

    This will keep track of the number of tasks that are received, or fail/succeed.

    Examples of matched lines:

    # 'success' example
    # [2013-02-09 15:20:43,779: INFO/MainProcess] Task entity.tasks.add_love[c8411104-ee40-49e8-ab4d-af1be60f93aa] succeeded in 0.169150829315s: None

    # 'received' example
    # [2015-07-20 18:25:59,371: INFO/MainProcess] Received task: appratings.tasks.add[6cd42812-7a9e-49d5-9bbd-1174233441cb]

    # 'scheduler' example
    # [2015-07-20 18:24:18,036: INFO/MainProcess] Scheduler: Sending due task add-every-5-seconds (appratings.tasks.add)

    # 'error' example
    # [2013-02-06 14:02:02,435: WARNING/MainProcess] len() on an unsliced queryset is not allowed

"""

from datetime import datetime
import re

import common

def parse_celery(logging, line):
    """ Attempt to match the line to one of three regexes. If a match is found, parse
        the line and return the metric to datadog.
    """
    success_regex = "\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.*?(\d+)).*Task (?P<task>[\w\.]+)\[(?P<task_id>[\w\-]+)\] succeeded in (?P<duration>\d+\.\d+)s: (?P<msg>\S+$)"

    received_regex = "\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.*?(\d+)).*Received task: (?P<task>[\w\.]+)\[(?P<task_id>[\w\-]+)\]"

    scheduler_regex = "\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.*?(\d+)).*Scheduler: Sending due task (?P<task_name>[\w\-\.]+) \((?P<task>.*?)\)"

    error_regex = "\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.*?(\d+)).*"

    attr_dict = {
        "metric_type": "counter",
        "unit": "request"
    }

    # Task success?
    match = re.match(success_regex, line)
    if match:
        event = match.groupdict()
        celery_event = "success"
        # could also return task name as an attribute?
        return ("celery.%s.%s" % (celery_event, event['task']),
            common.parse_date(event['timestamp']),
            1, # metric count
            attr_dict
        )

    # Task received?
    match = re.match(received_regex, line)
    if match:
        event = match.groupdict()
        celery_event = "received"
        return ("celery.%s.%s" % (celery_event, event['task']),
            common.parse_date(event['timestamp']),
            1, # metric count
            attr_dict
        )

    # Task scheduled?
    match = re.match(scheduler_regex, line)
    if match:
        event = match.groupdict()
        celery_event = "scheduler"
        return ("celery.%s.%s" % (celery_event, event['task']),
            common.parse_date(event['timestamp']),
            1, # metric count
            attr_dict
        )

    # If neither of those matched, it was probably an error. Unfortunately we only
    # get one line here, so we can't look at the whole traceback.
    match = re.match(error_regex, line)
    if match:
        event = match.groupdict()
        celery_event = "error"
        return ("celery.%s" % celery_event,
            common.parse_date(event['timestamp']),
            1, # metric count
            attr_dict
        )


def test_success(logging):
    """ Test success line.
    """    
    success_input = "[2013-02-09 15:20:43,779: INFO/MainProcess] Task entity.tasks.add_love[c8411104-ee40-49e8-ab4d-af1be60f93aa] succeeded in 0.169150829315s: None"
    expected = (
        "celery.success.entity.tasks.add_love",
        "1360452043",
        1,
        {"metric_type": "counter",
         "unit": "request"}
    )
    actual = parse_celery(logging, success_input)
    assert expected == actual, "%s != %s" % (expected, actual)


def test_received(logging):
    """ Test received line.
    """
    received_input = "[2015-07-20 18:25:59,371: INFO/MainProcess] Received task: appratings.tasks.add[6cd42812-7a9e-49d5-9bbd-1174233441cb]"
    expected = (
        "celery.received.appratings.tasks.add",
        "1437441959",
        1,
        {"metric_type": "counter",
         "unit": "request"}
    )
    actual = parse_celery(logging, received_input)
    assert expected == actual, "%s != %s" % (expected, actual)

def test_scheduler(logging):
    """ Test scheduler line.
    """
    received_input = "[2015-07-20 18:24:18,036: INFO/MainProcess] Scheduler: Sending due task add-every-5-seconds (appratings.tasks.add)"
    expected = (
        "celery.scheduler.appratings.tasks.add",
        "1437441858",
        1,
        {"metric_type": "counter",
         "unit": "request"}
    )
    actual = parse_celery(logging, received_input)
    assert expected == actual, "%s != %s" % (expected, actual)


def test_error(logging):
    """ Test error line.
    """
    error_input = "[2013-02-06 14:02:02,435: WARNING/MainProcess] len() on an unsliced queryset is not allowed"
    expected = (
        "celery.error",
        "1360188122",
        1,
        {"metric_type": "counter",
         "unit": "request"}
    )
    actual = parse_celery(logging, error_input)
    assert expected == actual, "%s != %s" % (expected, actual)

def test():
    # Set up the test logger
    import logging 
    logging.basicConfig(level=logging.DEBUG)

    test_success(logging)
    print "success: passed"
    test_received(logging)
    print "received: passed"
    test_scheduler(logging)
    print "scheduler: passed"
    test_error(logging)
    print "error: passed"


if __name__ == '__main__':
    # For local testing, callable as "python /path/to/parsers.py"
    test()