"""

Copyright 2019 BBC. Licensed under the terms of the Apache License 2.0.

"""
import pytest

from foxglove.connectors.base import DataConnector
from foxglove.connect import Connect

@pytest.fixture
def fake_model():

    class FakeModel:
        insects = Connect(engine_url="fake://bugsDB")
    
        def __init__(self):
            self._connections = {}

    return FakeModel


def test_connect_standalone():
    """
    :class:`foxglove.Connect` can be used outside of the ETL so data discovery can use the same
    way of working as full :class:`foxglove.Model`s.
    """
    # happy path
    # it works without Connect being part of a foxglove.Model
    c = Connect(engine_url="fake://MyDataset")
    assert c.data[0] == {'fake': 'data'}


def test_connect_spare_kwargs():
    """
    subclasses of :class:`foxglove.connectors.base.DataConnector` can be given specific/custom
    kwargs. An exception should be raised when unclaimed spare kwargs remain. This will make
    it harder for users to make mistakes and typos referring to arguments that never come
    into play.
    """
    c = Connect(engine_url="fake://foo", doesntexist='oh dear')
    with pytest.raises(ValueError):
        # the kwargs are not used until an engine_url is needed
        c._prepare_connection()

def test_connect_within_instantiated_class(fake_model):
    """
    Connect used as a class variable. The parent class, which in practice will be a
    :class:`foxglove.Model`.
    When used as a class variable in an instantiated class, Connect() will store information
    about the dataset within the parent (i.e. Model) class.
    """
    e0 = fake_model()
    assert len(e0._connections) == 0

    # connect on demand/access
    assert e0.insects is not None
    assert len(e0._connections) == 1

def test_connect_within_class(fake_model):
    """
    Connect used as a class variable. On access it returns a new instance that is separated,
    i.e. not the same object as, the original.
    """
    copy_0 = fake_model.insects
    copy_1 = fake_model.insects
    
    assert id(copy_0) != id(copy_1)


def test_custom_kwargs_are_passed():
    """
    foxglove.Connect should relay kwargs to subclasses of DataConnecter
    """
    # using bigquery because it has custom 'credentials' kwarg
    engine_url='bigquery://projectId=my_project;datasetId=nice_food;tableId=cakes;'
    c = Connect(engine_url=engine_url, credentials="hello_world")
    # on demand connection
    assert c.data is not None
    assert c._local_dataset.credentials == "hello_world"
