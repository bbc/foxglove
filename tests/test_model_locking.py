"""

Copyright 2019 BBC. Licensed under the terms of the Apache License 2.0.

"""
import os

from foxglove.lock_doc import LockDoc
from examples.deadly_animals import DeadlyAnimals

EXAMPLE_MODELS_DIR = os.path.dirname(os.path.abspath(__file__)) + '/../examples/'


def todo_test_get_lock_info():
    """
    Get locking info for the example :class:`PoisonousAnimals` model.
    """
    # relative path is used in connection
    current_working_directory = os.getcwd()
    os.chdir(EXAMPLE_MODELS_DIR)

    m = DeadlyAnimals()
    lock_doc = LockDoc(m)

    data_sources = lock_doc.get_data_sources()
    # TODO fake engine url should be from poisonous_animals' dataset discovery. The
    # dd should resolve to a url.
    assert 'fake://example.com/abc' == data_sources['deadly_animals']['engine_url']

    code_dependencies = lock_doc.get_code_dependencies()
    # these are whatever the machine running this test has installed. Not a good test as pip
    # might not even be installed. TODO - make a decision if silence on not finding pip is
    # a good idea.
    assert len(code_dependencies) >= 0

    # TODO when this code is in a git repo, check get_code_references() gives something useful.
    code_refs = lock_doc.get_code_references()
    # check there is some sort of git commitish
    # this test will fail if current project isn't in git. i.e. the example model is in
    # the current project.
    assert len(code_refs['git']['commit_ish']) == 40

    os.chdir(current_working_directory)

def todo_test_relock_dataset():
    """
    Use a lockfile to change the sqlite database file used by the example
    :class:`PoisonousAnimals` model. 
    """
    # TODO this. relocking
    m = DeadlyAnimals()

    # start condition
    assert 'fake://example.com/abc' == m.poisonous_animals.engine_url

    example_lock_info = """
    {"data_connections":
        {    "deadly_animals": { "read_access": true,
                            "write_access": false,
                            "ref": "BirdLife",
                            "engine_url": "sqlite:///wildlife_test_20190325.db",
                            "schema_name": "wildlife_schemas.WildLife"
                        }
        }
    }
"""
    lock_doc = LockDoc(m)
    lock_doc.relock(example_lock_info)

    # New DB has been set
    assert 'sqlite:///wildlife_test_20190325.db' == m.bird_life.engine_url

