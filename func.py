#
# oci-moogsoft-observability version 1.0.
#
# Copyright (c) 2022, Oracle and/or its affiliates. All rights reserved.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

import io
import json
import logging
import os
import re
import requests
from datetime import datetime

"""
    This sample OCI Function maps OCI Monitoring Service Raw Metrics to the MoogSoft 
    See https://api.docs.moogsoft.com/docs/latest/branches/main/165686df52254-metrics-integration-api-overview
    See https://api.docs.moogsoft.com/docs/latest/branches/main/7e62a06cd4faa-metric-datum-api-object
"""

# Use OCI Application or Function configurations to override these environment variable defaults.

api_endpoint = os.getenv('API_ENDPOINT', 'not-configured')
api_key = os.getenv('API_KEY', 'not-configured')
is_forwarding = eval(os.getenv('FORWARDING_ENABLED', "False"))

tag_keys = os.getenv('TAG_KEYS', 'name, namespace, displayName, resourceDisplayName, unit')
tag_set = set()

# Set all registered loggers to the configured log_level

logging_level = os.getenv('LOGGING_LEVEL', 'INFO')
loggers = [logging.getLogger()] + [logging.getLogger(name) for name in logging.root.manager.loggerDict]
[logger.setLevel(logging.getLevelName(logging_level)) for logger in loggers]

# Constants

TEN_MINUTES_SEC = 10 * 60
ONE_HOUR_SEC = 60 * 60

"""
"""


def handler(ctx, data: io.BytesIO = None):
    """
    OCI Function Entry Point
    :param ctx: InvokeContext
    :param data: data payload
    :return: plain text response indicating success or error
    """

    preamble = " {} / event count = {} / logging level = {} / forwarding to MoogSoft = {}"

    try:
        metrics_list = json.loads(data.getvalue())
        logging.info(preamble.format(ctx.FnName(), len(metrics_list), logging_level, is_forwarding))

        # logging.debug(metrics_list)

        transformed_event_list = transform_metric_events(event_list=metrics_list)
        send_to_moogsoft_endpoint(event_list=transformed_event_list)

    except (Exception, ValueError) as ex:
        logging.error('error handling logging payload: {}'.format(str(ex)))
        logging.error(ex)


def transform_metric_events(event_list):
    """
    :param event_list: the list of metric formatted log records.
    :return: the list of MoogSoft formatted log records
    """

    result_list = []
    for event in event_list:
        single_result = transform_metric_to_moogsoft_format(log_record=event)
        result_list.append(single_result)
        logging.debug(single_result)

    return result_list


def transform_metric_to_moogsoft_format(log_record: dict):
    """
    Transform metrics to MoogSoft format.
    MoogSoft is expecting Unix time ... see https://en.wikipedia.org/wiki/Unix_time
    :param log_record: metric log record
    :return: MoogSoft formatted log record
    """

    payload = []
    data_points = get_data_points(log_record)
    for dp in data_points:
        transformed_record = {
                'metric': get_dictionary_value(log_record, 'displayName'),
                'source': get_source(log_record),
                'time': dp.get('timestamp'),
                'data': dp.get('value'),
                'tags': get_tags(log_record),
            }
        payload.append(transformed_record)

    return payload


def get_source(log_record: dict):
    """
    Assembles a metric name that is compatible with MoogSoft.
    :param log_record:
    :return:
    """

    elements = get_dictionary_value(log_record, 'namespace').split('_')
    elements += camel_case_split(get_dictionary_value(log_record, 'name'))
    elements = [element.lower() for element in elements]
    return '.'.join(elements)


def camel_case_split(string):
    """
    :param string:
    :return: Splits camel case string to individual strings
    """

    return re.findall(r'[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))', string)


def get_now_timestamp():
    return datetime.now().timestamp()


def get_data_points(log_record: dict):
    """
    :param log_record:
    :return: an array of arrays where each array is a datapoint scalar pair
    """

    result = []

    datapoints = get_dictionary_value(dictionary=log_record, target_key='datapoints')
    for point in datapoints:
        data_point = {'timestamp': point.get('timestamp'), 'value': point.get('value')}

        result.append(data_point)

    return result


def get_tags(log_record: dict):
    """
    Assembles tags from selected metric attributes.
    See https://docs.MoogSofthq.com/getting_started/tagging/
    :param log_record: the log record to scan
    :return: string of comma-separated, key:value pairs matching MoogSoft tag format
    """

    result = []

    for tag in get_tag_set():
        value = get_dictionary_value(dictionary=log_record, target_key=tag)
        if value is None:
            continue

        if isinstance(value, str) and ':' in value:
            logging.warning('tag contains a \':\' / ignoring {} ({})'.format(tag, value))
            continue

        tag = '{}:{}'.format(tag, value)
        result.append(tag)

    return result


def get_tag_set():
    """
    :return: the set metric payload keys that are to be converted to a tag.
    """

    global tag_set

    if len(tag_set) == 0 and tag_keys:
        split_and_stripped_tags = [x.strip() for x in tag_keys.split(',')]
        tag_set.update(split_and_stripped_tags)
        logging.debug("tag key set / {} ".format(tag_set))

    return tag_set


def send_to_moogsoft_endpoint(event_list):
    """
    Sends each transformed event to API Endpoint.
    :param event_list: list of events in Moogsoft format
    :return: None
    """

    if is_forwarding is False:
        logging.info("MoogSoft forwarding is disabled - nothing sent")
        logging.info(json.dumps(event_list, indent=2))
        return

    # creating a session and adapter to avoid recreating
    # a new connection pool between each POST call

    try:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
        session.mount('https://', adapter)

        for event in event_list:
            api_headers = {'Content-type': 'application/json', 'apiKey': api_key}
            logging.debug("json to MoogSoft: {}".format(json.dumps(event)))
            response = session.post(api_endpoint, data=json.dumps(event), headers=api_headers)

            if response.status_code not in (200, 201, 202):
                raise Exception('error {} sending to MoogSoft: {}'.format(response.status_code, response.reason))

    finally:
        session.close()


def get_dictionary_value(dictionary: dict, target_key: str):
    """
    Recursive method to find value within a dictionary which may also have nested lists / dictionaries.
    :param dictionary: the dictionary to scan
    :param target_key: the key we are looking for
    :return: If a target_key exists multiple times in the dictionary, the first one found will be returned.
    """

    if dictionary is None:
        raise Exception('dictionary None for key'.format(target_key))

    target_value = dictionary.get(target_key)
    if target_value:
        return target_value

    for key, value in dictionary.items():
        if isinstance(value, dict):
            target_value = get_dictionary_value(dictionary=value, target_key=target_key)
            if target_value:
                return target_value

        elif isinstance(value, list):
            for entry in value:
                if isinstance(entry, dict):
                    target_value = get_dictionary_value(dictionary=entry, target_key=target_key)
                    if target_value:
                        return target_value


def local_test_mode(filename):
    """
    This routine reads a local json metrics file, converting the contents to MoogSoft format.
    :param filename: cloud events json file exported from OCI Logging UI or CLI.
    :return: None
    """

    logging.info("local testing started")

    with open(filename, 'r') as f:
        transformed_results = list()

        for line in f:
            event = json.loads(line)
            logging.debug(json.dumps(event, indent=4))
            transformed_results += transform_metric_to_moogsoft_format(event)

        logging.debug(json.dumps(transformed_results, indent=4))
        send_to_moogsoft_endpoint(event_list=transformed_results)

    logging.info("local testing completed")


"""
Local Debugging 
"""

if __name__ == "__main__":
    local_test_mode('oci-metrics-test-file.json')
