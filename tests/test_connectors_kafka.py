"""

Copyright 2019 BBC. Licensed under the terms of the Apache License 2.0.

"""
from datetime import datetime
import pytest

from foxglove.connectors.kafka_connector import KafkaConnector

EXAMPLE_ENGINE_URL_0="kafka://bionic/topic=foobar;start=@(2019-05-15 08:00:00);end=@(2019-05-15 18:00:00);"
EXAMPLE_ENGINE_URL_1="kafka://bionic/topic=foobar;start=@(2019-05-15 15:50:18);end=@(2019-05-15 15:50:24);"
EXAMPLE_ENGINE_URL_2="kafka://bionic/topic=uas;start=@(2019-05-22 10:42:00);end=@(2019-05-22 10:44:00);"

def test_engine_decode():
    date_format = "%Y-%m-%d %H:%M:%S"
    c = KafkaConnector(engine_url=EXAMPLE_ENGINE_URL_0)
    bootstrap_server, topic, start_params, end_params = c._decode_engine_url()
    assert bootstrap_server == 'bionic'
    assert topic == 'foobar'
    assert start_params == datetime.strptime("2019-05-15 08:00:00", date_format)
    assert end_params == datetime.strptime("2019-05-15 18:00:00", date_format)
 
@pytest.mark.integration
def test_partition_ranges():
    c = KafkaConnector(engine_url=EXAMPLE_ENGINE_URL_1)
    p_ranges = set([x for x in c._partition_ranges()])
    # my values right now, need to be aligned with test data when there's an integration env
    expected = {(0, 0, 233303), (1, 0, 133271), (2, 0, 133526)}
    assert expected == p_ranges
 
@pytest.mark.integration
def test_subset_of_items():
    """
    Use a date range to get some items from a topic.
    """
    c = KafkaConnector(engine_url=EXAMPLE_ENGINE_URL_2)
    some_items = [x for x in c]
    # TODO there is a problem with .offsets_for_times(tx)
    # there should be a few hundred items here, not thousands
    assert len(some_items) == 100

@pytest.mark.integration
def test_partition_ranges_2():
    c = KafkaConnector(engine_url=EXAMPLE_ENGINE_URL_2)
    p_ranges = set([x for x in c._partition_ranges()])
    # my values right now, need to be aligned with test data when there's an integration env
    expected = {(0, 0, 91)}
    assert expected == p_ranges

