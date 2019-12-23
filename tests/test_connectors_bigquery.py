"""

Copyright 2019 BBC. Licensed under the terms of the Apache License 2.0.

"""
from unittest.mock import Mock

import pytest
from google.cloud.bigquery import Client

from foxglove.connectors.bigquery import BigQueryConnector


@pytest.fixture
def fake_bq_client():
    return Mock(spec=Client(project='test_project'))

@pytest.mark.integration
def test_valid_bigquery_connector_init():
    connector = BigQueryConnector(
        'test_dataset_id',
        'test_table_id',
        'test_role'
    )
    assert connector.bq_dataset_id
    assert connector.bq_table_id
    assert connector.bq_client

@pytest.mark.integration
def test_write_truncate_ndjson_file(fake_bq_client):
    connector = BigQueryConnector(
        'test_dataset_id',
        'test_table_id',
        'test_role'
    )
    connector.bq_client = fake_bq_client
    connector.write_truncate_ndjson_file('test_ndjson_fh')
    fake_bq_client.load_table_from_file.assert_called_with(
        file_obj='test_ndjson_fh',
        destination=connector._bq_table,
        job_config=connector._job_config
    )

@pytest.mark.integration
def test_bq_table(fake_bq_client):
    connector = BigQueryConnector(
        'test_dataset_id',
        'test_table_id',
        'test_role'
    )
    connector.bq_client = fake_bq_client
    _ = connector._bq_table()
    connector._bq_dataset.table.assert_called_once()

@pytest.mark.integration
def test_bq_dataset(fake_bq_client):
    connector = BigQueryConnector(
        'test_dataset_id',
        'test_table_id',
        'test_role'
    )
    connector.bq_client = fake_bq_client
    _ = connector._bq_dataset()
    fake_bq_client.create_dataset.assert_called_once()

def test_bigquery_engine_url_decode():
    engine_url='bigquery://projectId=my_project;datasetId=nice_food;tableId=cakes;'
    connector = BigQueryConnector(engine_url=engine_url)
    project, dataset, table = connector._decode_engine_url()
    assert project == 'my_project'
    assert dataset == 'nice_food'
    assert table == 'cakes'

@pytest.mark.integration
def test_sql_query_with_params():
    engine_url='bigquery://projectId=bbc-datalab;datasetId=foxglove_test;tableId=rms_titles;'
    connector = BigQueryConnector(engine_url=engine_url)
    # check known value in sample data
    sql = "SELECT id FROM `bbc-datalab.foxglove_test.rms_titles` WHERE pid=@my_pid"
    for row in connector.query(sql=sql, sql_params=[("my_pid", "STRING", "b01qw8tz")]):
        assert row.id == 1

