"""

Copyright 2019 BBC. Licensed under the terms of the Apache License 2.0.

"""

from foxglove.connectors.bigquery import BigQueryConnector
from foxglove.connectors.fake import FakeDataConnector
from foxglove.connectors.flowerpot import FlowerPotConnector
from foxglove.connectors.kafka_connector import KafkaConnector


class Connect:
    """
    Connect to a dataset. A dataset contains :method:`data` and a :method:`schema` describing
    the data. An `engine_url` describes how to connect to the source of data.

    This as a descriptor. See https://docs.python.org/3/howto/descriptor.html

    Secrets management isn't yet implemented.
    """
    def __init__(self, **kwargs):
        """
        typical kwargs are 'ref', 'engine_url', 'access' TODO
        """
        # :class:`Connect` is responsible for resolving 'ref' into an engine_url via a
        # data catalogue. So 'ref' isn't passed to data type specific connectors (i.e.
        # subclasses of :class:`DataConnector`)
        self.ref = kwargs.pop('ref', None)
        self.relayed_kwargs = kwargs # these are passed to the data type specific connectors
        self._local_dataset = None # see :method:`data`

    def __call__(self, **kwargs):
        """
        Overlay (and overwrite) kwarg on existing instance.
        Factory style, returns new instance.
        TODO testing so not returning a new instance.
        """
        # TODO - this could be a clone, and should at least have all allowed_args, needs thought
        self.__init__(**kwargs)
        return self

    def __get__(self, instance, instance_class):
        if instance is None:
            # class method called
            return self.__class__(ref=self.ref)
        ident = id(self)
        if ident not in instance._connections:
            instance._connections[ident] = self._prepare_connection()
        return instance._connections[ident]

    def _prepare_connection(self):
        """
        Resolve everything apart from secrets needed to access the engine behind this dataset.
        """
        # TODO - available connectors shouldn't be hard coded based on url, the catalogue of
        #    available connectors should be decoupled and each should declare which engine types
        #    they support
        #
        if self.relayed_kwargs['engine_url'] is None:
            raise NotImplementedError(("Sorry! Dataset discovery (looking up engine_url from ref) "
                                      "hasn't been written yet."
                                      )
                                      )
        engine_url = self.relayed_kwargs['engine_url']
        engine_type = engine_url.split('://', 1)[0] + '://'
        for connector_cls in [BigQueryConnector, FlowerPotConnector, FakeDataConnector,
                              KafkaConnector]:
            if engine_type == connector_cls.engine_type:
                connector = connector_cls(**self.relayed_kwargs)
                break
        else:
            raise NotImplementedError(f"Unknown engine in url:{engine_url}")

        connector.uses_dataset_discovery = self.ref is not None
        return connector

    @property
    def data(self):
        """
        The data within the dataset the connection is to. It's structure could be described
        by :method:`schema`. This property is used when Connect() is used outside of an ETL
        model.
        """
        if self._local_dataset is None:
            self._local_dataset = self._prepare_connection()
        return self._local_dataset.data

    @property
    def schema(self):
        """
        The structure of the data within the dataset the connection is to.
        """
        # TODO fake
        raise NotImplementedError("TODO")

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    def __iter__(self):
        """
        more intuitive use of the data behind this Connect. i.e. proxy to a DataConnector.
        e.g.
        ...
        for record in Connect(ref="my_dataset"):
            print(record)
        """
        return self.data
