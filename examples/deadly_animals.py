"""

Copyright 2019 BBC. Licensed under the terms of the Apache License 2.0.

"""
from foxglove.model import Model
from foxglove.connect import Connect

class DeadlyAnimals(Model):
    """
    Use dataset discovery to connect to a dataset.
    """
    deadly_animals = Connect(ref='Deadly Animals')

    def build(self):
        for animal in self.deadly_animals:
            print(animal)

if __name__ == '__main__':
    m = DeadlyAnimals()
    m.go()
