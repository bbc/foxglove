"""

Copyright 2019 BBC. Licensed under the terms of the Apache License 2.0.

"""
import foxglove

class FlowerpotToBigquery(foxglove.Model):
    """
    Send ndjson files from within a flowerpot to BigQuery.
    """
    poisonous_animals = foxglove.Connect(engine_url='flowerpot://data/poisonous_animals.flowerpot')
    animals_database = foxglove.Connect(
        engine_url='bigquery://projectId=bbc-datalab;datasetId=foxglove_test;tableId=poisonous_animals;',
        access=foxglove.AccessMode.WRITE
        )

    def build(self):

        for file_name, ndjson_file_handle in self.poisonous_animals.flowerpot.file_handles():
            self.log(f"Sending {file_name} to BigQuery")
            self.animals_database.write_truncate_file(ndjson_file_handle)
            ndjson_file_handle.close()

        self.log("All done!")

if __name__ == '__main__':
    m = FlowerpotToBigquery()
    m.go()
