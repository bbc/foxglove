"""

Copyright 2019 BBC. Licensed under the terms of the Apache License 2.0.

"""
from foxglove.pinnate import Pinnate

def test_attrib_and_dict():
    a = Pinnate({'my_string':'abcdef'})
    assert a.my_string == 'abcdef'
    assert a['my_string'] == 'abcdef'
    assert a.as_dict() == {'my_string': 'abcdef'}

def test_recurse():
    d={'my_things' : [1,2,{'three':3}]}
    a = Pinnate(d)
    p = a.my_things
    assert p[0] == 1 and p[1] == 2 and isinstance(p[2], Pinnate)
    assert p[2].three == 3
