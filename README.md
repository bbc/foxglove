# Foxglove

Foxglove is an experimental Extract, Transform, Load (ETL) framework.

Copyright © 2019 BBC. Licensed under the terms of the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html).

## Quick start

In the project you’d like to use Foxglove in, run:-

```shell
pipenv shell
pipenv install -e git+https://github.com/bbc/foxflove.git
```


Run one of the examples:-

```shell
ln -s `pipenv --venv`/src/foxglove/foxglove/examples/
cd examples
python poisonous_animals.py  
```

The `ln` (symlink) command will make all the modules in the examples directory visible. You could just change into this directory with the command:-

```shell
cd `pipenv --venv`/src/foxglove/foxglove/examples
```


## Overview

A Foxglove ETL *model* inherits from `foxglove.model` and uses class level variables to declare *connectors* to the data it acts on.

Example:-

```python
import foxglove

class PoisonousAnimals(foxglove.Model):
    poisonous_animals = foxglove.Connect(engine_url='flowerpot://data/poisonous_animals.flowerpot')
```

When instantiated, `self.poisonous_animals` will be a *dataset* that can be ETL operations can be done on.

The `engine_url` parameter passed to `foxglove.Connect` is specifying the data type (flowerpot) and exact location for the data (`data/poisonous_animals.flowerpot` is a relative file path).

Instead of `engine_url` you could also specify a `ref` and this uses the data catalogue to lookup the `engine_url`.

`foxglove.Connect` is responsible for resolving the `ref` to an `engine_url` and passing this to a subclass of `foxglove.connectors.base.DataConnector` which can read and maybe write this data type.


## Unit tests

Ensure the working directory is the base for foxglove (same directory as the Pipfile):
```shell
pipenv run python3 -m pytest -m "not integration"
```
