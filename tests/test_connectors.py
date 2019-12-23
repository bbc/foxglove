"""

Copyright 2019 BBC. Licensed under the terms of the Apache License 2.0.

"""
import os
import tarfile
from unittest import mock
from unittest.mock import Mock, patch, MagicMock

import pytest
from google.auth.compute_engine import credentials
from google.cloud.storage import Bucket, Blob

from foxglove.connectors.base import AccessMode
from foxglove.connectors.gcs_flowerpot import GcsFlowerpotConnector
from foxglove.connectors.flowerpot import FlowerpotEngine, FlowerPotConnector

EXAMPLE_FLOWERPOT_PATH = os.path.dirname(os.path.abspath(__file__))\
    +'/../integration-tests/exampleflowerpot.tar.gz'

EXAMPLE_ENGINE_URL = 'gs+flowerpot://fake_flowerpot_bucket/some_file.json'

@pytest.fixture
def fake_bucket():
    return Mock(spec=Bucket)


@pytest.fixture
def fake_flowerpot_bucket():
    def fake_object_download(file_handle):
        original_file = open(EXAMPLE_FLOWERPOT_PATH, 'rb')
        file_handle.write(original_file.read())

    bucket = Mock(spec=Bucket)
    blob = Mock(spec=Blob)
    blob.download_to_file = fake_object_download
    bucket.get_blob.return_value = blob
    return bucket


@patch('google.cloud.storage.Client')
def test_valid_flowerpot_init(fake_storage_client, fake_flowerpot_bucket):
    fake_bucket = fake_flowerpot_bucket
    fake_storage_client.return_value.get_bucket.return_value = fake_bucket

    connector = GcsFlowerpotConnector(engine_url=EXAMPLE_ENGINE_URL)
    assert connector.data
    assert connector.schema is None


def test_flowerpot_deserialize():
    test_string = bytes(
        '{"availability": "apple", "referential": "raspberry"}\n{"availability": "anchor", "referential": "rudder"}',
        encoding='utf-8')
    result = FlowerpotEngine._deserialize_ndjson_string(test_string)
    assert result[0] == {"availability": "apple", "referential": "raspberry"}
    assert result[1] == {"availability": "anchor", "referential": "rudder"}


def test_iterate_over_json_lines():
    with tarfile.open(EXAMPLE_FLOWERPOT_PATH, 'r:gz') as tf:
        reader = FlowerpotEngine(tf)
        results = list(reader.items())
        assert len(results) == 4
        assert "availability" in results[0]
        assert "referential" in results[0]


@patch('google.cloud.storage.Client')
def test_gcs_flowerpot_data(fake_storage_client, fake_flowerpot_bucket):
    fake_bucket = fake_flowerpot_bucket
    fake_storage_client.return_value.get_bucket.return_value = fake_bucket

    connector = GcsFlowerpotConnector(engine_url=EXAMPLE_ENGINE_URL)
    all_data = list(connector.data)
    assert len(all_data) == 4


@pytest.fixture
def mock_credentials():
    return mock.Mock(spec=google.auth.credentials.Credentials)
 
 
@patch('google.cloud.storage.Client')
def test_friendly_flowerpot(fake_storage_client, fake_flowerpot_bucket):
    fake_bucket = fake_flowerpot_bucket
    fake_storage_client.return_value.get_bucket.return_value = fake_bucket
    flowerpot_connector = GcsFlowerpotConnector(engine_url=EXAMPLE_ENGINE_URL)
    data = list(flowerpot_connector.data)
    assert 'availability' in data[0]


@patch('google.cloud.storage.Client')
def test_gcs_credentials_passed_to_client(fake_storage_client: MagicMock):
    fake_credentials = credentials.Credentials()
    project = 'some_project'
    engine_url = f'gs+flowerpot://{project}.fake_flowerpot_bucket/some_file.json'
    GcsFlowerpotConnector(engine_url=engine_url,
                          access=AccessMode.READ,
                          credentials=fake_credentials)
    fake_storage_client.assert_called_with(project, fake_credentials)

def test_flowerpot_all_items():
    """
    Iterate all the data items in all the files in the example flowerpot.
    """
    c = FlowerPotConnector(engine_url="flowerpot://"+EXAMPLE_FLOWERPOT_PATH)
    all_items = [(r.availability, r.referential) for r in c]
    all_items.sort()
    expected = "[('acoustic', 'rap'), ('anchor', 'rudder'), ('antenna', 'receive'), ('apple', 'raspberry')]"
    assert expected == str(all_items)

def test_flowerpot_query_one_file():
    """
    The 'table' kwarg gets rows from all files that start with that string.
    """
    c = FlowerPotConnector(engine_url="flowerpot://"+EXAMPLE_FLOWERPOT_PATH)
    some_items = [(r.availability, r.referential) for r in c.query(table='test_a')]
    some_items.sort()
    expected = "[('anchor', 'rudder'), ('apple', 'raspberry')]"
    assert expected == str(some_items)

