""" Datadog log parser for celery.

    This will keep track of the number of tasks that are received, or fail/succeed.

    Examples of matched lines:

    # 'success' example
    # [2013-02-09 15:20:43,779: INFO/MainProcess] Task entity.tasks.add_love[c8411104-ee40-49e8-ab4d-af1be60f93aa] succeeded in 0.169150829315s: None

    # 'received' example
    # [2013-02-09 15:20:44,261: INFO/MainProcess] Got task from broker: user.tasks.sync_follow_open_graph[aa7d0eec-5416-4d15-b36b-10f2d85375e9] eta:[2013-02-09 15:20:47.256882]

    # 'error' example
    # [2013-02-06 14:02:02,435: WARNING/MainProcess] len() on an unsliced queryset is not allowed

"""

from datetime import datetime
import re

def get_timestamp(date_string):
    """ Takes "2013-02-09 15:20:43", returns "1360423243".
    """
    return datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S").strftime("%s")

def parse_celery(logging, line):
    """ Attempt to match the line to one of three regexes. If a match is found, parse
        the line and return the metric to datadog.
    """
    success_regex = "\[(?P<time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*Task (?P<task_name>[\w\.]+)\[(?P<task_id>[\w\-]+)\] succeeded in (?P<duration>\d+\.\d+)s"

    received_regex = "\[(?P<time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*Got task from broker: (?P<task_name>[\w\.]+)\[(?P<task_id>[\w\-]+)\]"
    error_regex = "\[(?P<time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*"

    attr_dict = {
        "metric_type": "counter",
        "unit": "request"
    }

    # Task success?
    match = re.match(success_regex, line)
    if match:
        data = match.groupdict()
        # could also return task name as an attribute?
        return ("celery.tasks.success.%s" % data['task_name'],
            get_timestamp(data['time']),
            1, # metric count
            attr_dict
        )

    # Task received?
    match = re.match(received_regex, line)
    if match:
        data = match.groupdict()
        return ("celery.tasks.received.%s" % data['task_name'],
            get_timestamp(data['time']),
            1, # metric count
            attr_dict
        )

    # If neither of those matched, it was probably an error. Unfortunately we only
    # get one line here, so we can't look at the whole traceback.
    match = re.match(error_regex, line)
    if match:
        data = match.groupdict()
        return ("celery.tasks.error",
            get_timestamp(data['time']),
            1, # metric count
            attr_dict
        )


def test_success(logging):
    """ Test task success line.
    """
    
    success_input = "[2013-02-09 15:20:43,779: INFO/MainProcess] Task entity.tasks.add_love[c8411104-ee40-49e8-ab4d-af1be60f93aa] succeeded in 0.169150829315s: None"
    expected = (
        "celery.tasks.success.entity.tasks.add_love",
        "1360423243",
        1,
        {"metric_type": "counter",
         "unit": "request"}
    )
    actual = parse_celery(logging, success_input)
    assert expected == actual, "%s != %s" % (expected, actual)


def test_received(logging):
    """ Test task receive line.
    """
    received_input = "[2013-02-09 15:20:44,261: INFO/MainProcess] Got task from broker: user.tasks.sync_follow_open_graph[aa7d0eec-5416-4d15-b36b-10f2d85375e9] eta:[2013-02-09 15:20:47.256882]"
    expected = (
        "celery.tasks.received.user.tasks.sync_follow_open_graph",
        "1360423244",
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
        "celery.tasks.error",
        "1360159322",
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
    test_received(logging)
    test_error(logging)


if __name__ == '__main__':
    # For local testing, callable as "python /path/to/parsers.py"
    test()
