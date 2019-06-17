#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from logging import (Logger, getLogger)

from kafka import KafkaAdminClient
from kafka.cluster import ClusterMetadata
from kafka.errors import (KafkaConfigurationError, NoBrokersAvailable, KafkaConnectionError)

from typing import List, Union

from tonga.services.coordinator.kafka_client.errors import (BadArgumentKafkaClient, KafkaClientConnectionErrors,
                                                            KafkaAdminConfigurationError, CurrentInstanceOutOfRange)

__all__ = [
    'KafkaClient'
]

logger: Logger = getLogger()


class KafkaClient:
    """ Class KafkaClient

    Contain all repetitive information, this class was used in KafkaConsumer / KafkaProducer / StoreBuilder

    Attributes:
        bootstrap_servers (Union[str, List[str]]): ‘host[:port]’ string (or list of ‘host[:port]’ strings) that
                                        the producer should contact to bootstrap initial cluster metadata.
                                        This does not have to be the full node list. It just needs to have at
                                        least one broker that will respond to a Metadata API Request.
                                        Default port is 9092. If no servers are specified, will default
                                        to localhost:9092.
    """
    bootstrap_servers: Union[str, List[str]]
    client_id: str
    cur_instance: int
    nb_replica: int

    _kafka_admin_client: KafkaAdminClient
    _cluster_metadata: ClusterMetadata

    def __init__(self, client_id: str, cur_instance: int = 0, nb_replica: int = 1,
                 bootstrap_servers: Union[str, List[str]] = None):
        """ KafkaClient constructor

        Args:
            bootstrap_servers (Union[str, List[str]]): ‘host[:port]’ string (or list of ‘host[:port]’ strings) that
                                        the producer should contact to bootstrap initial cluster metadata.
                                        This does not have to be the full node list. It just needs to have at
                                        least one broker that will respond to a Metadata API Request.
                                        Default port is 9092. If no servers are specified, will default
                                        to localhost:9092.
            client_id (str): Client name
            cur_instance: Current instance
            nb_replica: Number of replica
        """
        if bootstrap_servers is None:
            self.bootstrap_servers = 'localhost:9092'
        else:
            self.bootstrap_servers = bootstrap_servers

        if isinstance(client_id, str) and isinstance(nb_replica, int) and isinstance(cur_instance, int):
            self.client_id = client_id
            self.nb_replica = nb_replica
            self.cur_instance = cur_instance
        else:
            raise BadArgumentKafkaClient

        if self.cur_instance > self.nb_replica:
            raise CurrentInstanceOutOfRange

        try:
            self._kafka_admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers,
                                                        client_id=f'waiter-{self.cur_instance}')
            self._cluster_metadata = ClusterMetadata(bootstrap_servers=self.bootstrap_servers)
        except KafkaConfigurationError:
            raise KafkaAdminConfigurationError
        except (KafkaConnectionError, NoBrokersAvailable):
            raise KafkaClientConnectionErrors

    def get_kafka_admin_client(self) -> KafkaAdminClient:
        return self._kafka_admin_client

    def get_cluster_metadata(self) -> ClusterMetadata:
        return self._cluster_metadata
