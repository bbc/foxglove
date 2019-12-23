"""

Copyright 2019 BBC. Licensed under the terms of the Apache License 2.0.

"""
from collections import defaultdict

import foxglove


class PoisonousAnimals(foxglove.Model):
    """
    Super simple ETL.

    Just using normal python data structures, group all the poisonous animals by country where
    they are found.
    """
    poisonous_animals = foxglove.Connect(engine_url='flowerpot://data/poisonous_animals.flowerpot')

    def build(self):
        by_country = defaultdict(list)
        for animal in self.poisonous_animals:
            by_country[animal.where].append(animal.name)

        # Use log this so we can see it
        for country, animals in by_country.items():
            these_animals = ",".join(animals)
            msg = f"In {country} you could find {these_animals}"
            self.log(msg)


if __name__ == '__main__':
    m = PoisonousAnimals()
    m.go()
