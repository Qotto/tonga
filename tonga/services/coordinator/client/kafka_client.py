#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Contain KafkaClient.

This class manage configuration on each KafkaConsumer / KafkaProducer / StoreBuilder
"""

from logging import (Logger, getLogger)
from typing import List, Union

from kafka import KafkaAdminClient
from kafka.cluster import ClusterMetadata
from kafka.errors import (KafkaConfigurationError, NoBrokersAvailable, KafkaConnectionError)

from tonga.services.coordinator.client.base import BaseClient
from tonga.services.coordinator.client.errors import (BadArgumentKafkaClient, KafkaClientConnectionErrors,
                                                      KafkaAdminConfigurationError)

__all__ = [
    'KafkaClient'
]

logger: Logger = getLogger()


class KafkaClient(BaseClient):
    """ Class KafkaClient

    Contain all repetitive information, this class was used in KafkaConsumer / KafkaProducer / StoreBuilder

    Attributes:
        bootstrap_servers (Union[str, List[str]]): ‘host[:port]’ string (or list of ‘host[:port]’ strings) that
                                        the producer should contact to bootstrap initial cluster metadata.
                                        This does not have to be the full node list. It just needs to have at
                                        least one broker that will respond to a Metadata API Request.
                                        Default port is 9092. If no servers are specified, will default
                                        to localhost:9092.
        client_id (str): A name for this client. This string is passed in each request to servers and can be
                        used to identify specific server-side log entries that correspond to this client
        cur_instance: (int): Current service instance
        nb_replica (int): Number of service replica
    """
    bootstrap_servers: Union[str, List[str]]
    client_id: str
    cur_instance: int
    nb_replica: int

    _kafka_admin_client: KafkaAdminClient
    _cluster_metadata: ClusterMetadata

    def __init__(self, client_id: str, bootstrap_servers: Union[str, List[str]] = None, **kwargs) -> None:
        """ KafkaClient constructor

        Args:
            bootstrap_servers (Union[str, List[str]]): ‘host[:port]’ string (or list of ‘host[:port]’ strings) that
                                        the producer should contact to bootstrap initial cluster metadata.
                                        This does not have to be the full node list. It just needs to have at
                                        least one broker that will respond to a Metadata API Request.
                                        Default port is 9092. If no servers are specified, will default
                                        to localhost:9092.
            client_id (str): A name for this client. This string is passed in each request to servers and can be
                            used to identify specific server-side log entries that correspond to this client
            cur_instance: Current service instance
            nb_replica: Number of service replica

        Raises:
            BadArgumentKafkaClient: raised if argument are not valid
            CurrentInstanceOutOfRange: raised if current instance is highest than replica number
            KafkaAdminConfigurationError: raised if KafkaAdminClient argument are not valid
            KafkaClientConnectionErrors: raised if KafkaClient can't connect on kafka or no brokers are available
        """
        super().__init__(**kwargs)

        if bootstrap_servers is None:
            self.bootstrap_servers = 'localhost:9092'
        else:
            self.bootstrap_servers = bootstrap_servers

        if isinstance(client_id, str):
            self.client_id = client_id
        else:
            raise BadArgumentKafkaClient

        try:
            self._kafka_admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers,
                                                        client_id=f'waiter-{self.cur_instance}')
            self._cluster_metadata = ClusterMetadata(bootstrap_servers=self.bootstrap_servers)
        except KafkaConfigurationError:
            raise KafkaAdminConfigurationError
        except (KafkaConnectionError, NoBrokersAvailable):
            raise KafkaClientConnectionErrors

    def get_kafka_admin_client(self) -> KafkaAdminClient:
        """ Return KafkaAdminClient

        Returns:
            KafkaAdminClient
        """
        return self._kafka_admin_client

    def get_cluster_metadata(self) -> ClusterMetadata:
        """ Return ClusterMetadata

        Returns:
            ClusterMetadata
        """
        return self._cluster_metadata
