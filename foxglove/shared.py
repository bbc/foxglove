"""

Copyright 2019 BBC. Licensed under the terms of the Apache License 2.0.

"""
from enum import Enum

from foxglove.connectors.fake import FakeDataConnector


class LogLevel(Enum):
    DEBUG = 10
    PROGRESS = 20
    INFO = 30
    WARNING = 40
    ERROR = 50
    SEVERE = 60


def model_connections(model_cls, access=None):
    """
    Generator yielding connections from a model.
    Just for the DAG demo, delete this or put is somewhere proper afterwards.
    """
    for obj_name in dir(model_cls):
        obj = getattr(model_cls, obj_name)
        # TODO access by class isn't working because access mode isn't preserved?
        if isinstance(obj, FakeDataConnector):
            if access is not None and not obj.access == access:
                continue
            yield obj
