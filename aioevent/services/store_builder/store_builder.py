#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from asyncio import AbstractEventLoop
from aiokafka import TopicPartition
from kafka.cluster import ClusterMetadata
from kafka.admin import KafkaAdminClient

from typing import List

from aioevent.services.store_builder.base import BaseStoreBuilder

from aioevent.services.stores.local.base import BaseLocalStore
from aioevent.services.stores.globall.base import BaseGlobalStore

from aioevent.services.consumer.consumer import KafkaConsumer
from aioevent.services.producer.producer import KafkaProducer

from aioevent.model.exceptions import StoreKeyNotFound

__all__ = [
    'StoreBuilder'
]


class StoreBuilder(BaseStoreBuilder):
    """
    StoreBuilder Class

    Attributes:
        name (str): StoreBuilder name
        _current_instance (int): Current service instance
        _nb_replica (int): Number of service instance
        _topic_store (str): Name topic where store event was send
        _rebuild (bool): If is true store is rebuild from first offset of topic/partition
        _local_store (BaseLocalStore): Local store instance Memory / Shelve / RockDB / ...
        _global_store (BaseGlobalStore): Global store instance Memory / Shelve / RockDB / ...
        _cluster_metadata (ClusterMetadata): ClusterMetadata from kafka-python go to for more details
        _cluster_admin (KafkaAdminClient): KafkaAdminClient from kafka-python go to for more details
        _loop (AbstractEventLoop): Asyncio loop
        _store_consumer (KafkaConsumer): KafkaConsumer go to for more details
        _store_producer (KafkaProducer): KafkaProducer go to for more details
        _event_sourcing: If is true StateBuilder block instance for write in local & global store, storage will
                            be only updated by handle store function, more details in StorageBuilder.
                            Otherwise instance can only write in own local store, global store is only read only

        _stores_partitions (List[TopicPartition]): List of topics/partitions
    """

    name: str
    _current_instance: int
    _nb_replica: int
    _topic_store: str
    _rebuild: bool
    _event_sourcing: bool

    _local_store: BaseLocalStore
    _global_store: BaseGlobalStore

    _cluster_metadata: ClusterMetadata
    _cluster_admin: KafkaAdminClient

    _loop: AbstractEventLoop

    _store_consumer: KafkaConsumer
    _store_producer: KafkaProducer

    _stores_partitions: List[TopicPartition]

    def __init__(self, store_builder_name: str, current_instance: int, nb_replica: int, topic_store: str,
                 local_store: BaseLocalStore, global_store: BaseGlobalStore, cluster_metadata: ClusterMetadata,
                 cluster_admin: KafkaAdminClient, loop: AbstractEventLoop, rebuild: bool = False,
                 event_sourcing: bool = False) -> None:
        """
        StoreBuilder constructor

        Args:
            store_builder_name: StoreBuilder name
            current_instance: Current service instance
            nb_replica: Number of service instance
            topic_store: Name topic where store event was send
            local_store: Local store instance Memory / Shelve / RockDB / ...
            global_store: Global store instance Memory / Shelve / RockDB / ...
            cluster_metadata: ClusterMetadata from kafka-python go to for more details
            cluster_admin: KafkaAdminClient from kafka-python go to for more detail
            loop: Asyncio loop
            rebuild: If is true store is rebuild from first offset of topic / partition
            event_sourcing: If is true StateBuilder block instance for write in local & global store, storage will
                            be only updated by handle store function, more details in StorageBuilder.
                            Otherwise instance can only write in own local store, global store is only read only
        """
        self.name = store_builder_name
        self._current_instance = current_instance
        self._nb_replica = nb_replica
        self._rebuild = rebuild
        self._event_sourcing = event_sourcing

        self._topic_store = topic_store

        self._local_store = local_store
        self._global_store = global_store

        self._cluster_metadata = cluster_metadata
        self._cluster_admin = cluster_admin

        self._loop = loop

        self._store_consumer = ...  # type: ignore
        self._store_producer = ...  # type: ignore

        self._stores_partitions = list()

    def initialize_store_builder(self) -> None:
        ...

    def get_local_store(self) -> BaseLocalStore:
        return self._local_store

    def get_global_store(self) -> BaseGlobalStore:
        return self._global_store

    def get_current_instance(self) -> int:
        return self._current_instance

    def is_event_sourcing(self) -> bool:
        return self._event_sourcing
