"""

Copyright 2019 BBC. Licensed under the terms of the Apache License 2.0.

"""
import setuptools


long_description = """Foxglove is an experimental Extract, Transform, Load (ETL) framework.
"""

setuptools.setup(
    name="foxglove-bbc",
    version="0.0.2",
    author="BBC Datalab",
    author_email="datalab@bbc.co.uk",
    description="ETL framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/bbc/foxglove",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'google-cloud-bigquery',
        'google-cloud-storage',
        'ndjson',
        'gitpython',
        'kafka-python',
        ],
)
