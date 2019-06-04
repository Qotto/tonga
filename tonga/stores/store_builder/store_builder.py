#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import asyncio
import logging
from logging import Logger
from asyncio import AbstractEventLoop
from aiokafka import TopicPartition
from aiokafka.producer.message_accumulator import RecordMetadata
from kafka.cluster import ClusterMetadata
from kafka.admin import KafkaAdminClient

from typing import List

# Store Builder import
from tonga.stores.store_builder.base import BaseStoreBuilder

# Serializer import
from tonga.services.serializer.base import BaseSerializer

# StatefulsetPartitioner import
from tonga.services.coordinator.partitioner.statefulset_partitioner import StatefulsetPartitioner

# Stores import
from tonga.stores.local.base import BaseLocalStore
from tonga.stores.local.memory import LocalStoreMemory
from tonga.stores.globall.base import BaseGlobalStore
from tonga.stores.globall.memory import GlobalStoreMemory

# Consumer & Producer import
from tonga.services.consumer.kafka_consumer import KafkaConsumer
from tonga.services.producer.kafka_producer import KafkaProducer

# Storage Builder import
from tonga.models.store_record.store_record import StoreRecord

# Import store builder exceptions
from tonga.stores.store_builder.errors import (UninitializedStore, CanNotInitializeStore, FailToSendStoreRecord)
# Import store exceptions
from tonga.stores.errors import StoreKeyNotFound
# Import consumer exceptions
from tonga.services.consumer.errors import (OffsetError, TopicPartitionError, NoPartitionAssigned)
# Import producer exceptions
from tonga.services.producer.errors import (KeyErrorSendEvent, ValueErrorSendEvent,
                                               TypeErrorSendEvent, FailToSendEvent)

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

    _logger: Logger

    _stores_partitions: List[TopicPartition]

    def __init__(self, name: str, current_instance: int, nb_replica: int, topic_store: str,
                 serializer: BaseSerializer, local_store: BaseLocalStore, global_store: BaseGlobalStore,
                 bootstrap_server: str, cluster_metadata: ClusterMetadata, cluster_admin: KafkaAdminClient,
                 loop: AbstractEventLoop, rebuild: bool = False, event_sourcing: bool = False) -> None:
        """
        StoreBuilder constructor

        Args:
            name: StoreBuilder name
            current_instance: Current service instance
            nb_replica: Number of service instance
            topic_store: Name topic where store event was send
            serializer: Serializer, this param was sends by tonga
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
        self.name = name
        self._current_instance = current_instance
        self._nb_replica = nb_replica
        self._rebuild = rebuild
        self._event_sourcing = event_sourcing
        self._bootstrap_server = bootstrap_server

        self._serializer = serializer

        self._topic_store = topic_store

        self._local_store = local_store
        self._global_store = global_store

        self._cluster_metadata = cluster_metadata
        self._cluster_admin = cluster_admin

        self._logger = logging.getLogger('tonga')
        self._loop = loop

        self._store_consumer = KafkaConsumer(name=f'{self.name}_consumer', serializer=self._serializer,
                                             bootstrap_servers=self._bootstrap_server, auto_offset_reset='earliest',
                                             client_id=f'{self.name}_consumer_{self._current_instance}',
                                             topics=[self._topic_store], group_id=f'{self.name}_consumer',
                                             loop=self._loop, isolation_level='read_committed',
                                             assignors_data={'instance': self._current_instance,
                                                             'nb_replica': self._nb_replica,
                                                             'assignor_policy': 'all'},
                                             store_builder=self)

        partitioner = StatefulsetPartitioner(instance=self._current_instance)
        self._store_producer = KafkaProducer(name=f'{self.name}_producer', bootstrap_servers=self._bootstrap_server,
                                             client_id=f'{self.name}_producer_{self._current_instance}',
                                             partitioner=partitioner,
                                             loop=self._loop, serializer=self._serializer, acks='all')

        self._stores_partitions = list()

    def return_consumer_task(self):
        return asyncio.ensure_future(self._store_consumer.listen_store_records(self._rebuild), loop=self._loop)

    async def initialize_store_builder(self) -> None:
        """
        Initializes store builder, connect local & global store with tonga consumer.
        This function seek to last committed offset if store_metadata exist.

        Returns:
            None
        """
        # Initialize local store
        self._logger.info('Start initialize store builder')
        if isinstance(self._local_store, LocalStoreMemory):
            # If _local_store is an instance from LocalStoreMemory, auto seek to earliest position for rebuild
            self._logger.info('LocalStoreMemory seek to earliest')
            assigned_partitions = list()
            last_offsets = dict()
            assigned_partitions.append(TopicPartition(self._topic_store, self._current_instance))
            last_offsets[TopicPartition(self._topic_store, self._current_instance)] = 0
            await self._local_store.set_store_position(self._current_instance, self._nb_replica, assigned_partitions,
                                                       last_offsets)
            try:
                await self._store_consumer.load_offsets('earliest')
            except (TopicPartitionError, NoPartitionAssigned) as err:
                self._logger.exception(f'{err.__str__()}')
                raise CanNotInitializeStore
        else:
            try:
                # Try to get local_store_metadata, seek at last read offset
                local_store_metadata = await self._local_store.get_metadata()
            except StoreKeyNotFound:
                # If metadata doesn't exist in DB, auto seek to earliest position for rebuild
                assigned_partitions = list()
                last_offsets = dict()
                assigned_partitions.append(TopicPartition(self._topic_store, self._current_instance))
                last_offsets[TopicPartition(self._topic_store, self._current_instance)] = 0
                await self._local_store.set_store_position(self._current_instance, self._nb_replica,
                                                           assigned_partitions, last_offsets)
                try:
                    await self._store_consumer.load_offsets('earliest')
                except (TopicPartitionError, NoPartitionAssigned) as err:
                    self._logger.exception(f'{err.__str__()}')
                    raise CanNotInitializeStore
            else:
                # If metadata is exist in DB, , auto seek to last position
                try:
                    last_offset = local_store_metadata.last_offsets[
                        TopicPartition(self._topic_store, self._current_instance)]
                    await self._store_consumer.seek_custom(self._topic_store, self._current_instance, last_offset)
                except (OffsetError, TopicPartitionError, NoPartitionAssigned) as err:
                    self._logger.exception(f'{err.__str__()}')
                    raise CanNotInitializeStore
                await self._local_store.set_store_position(self._current_instance, self._nb_replica,
                                                           local_store_metadata.assigned_partitions,
                                                           local_store_metadata.last_offsets)

        # Initialize global store
        if isinstance(self._global_store, GlobalStoreMemory):
            # If _global_store is an instance from GlobalStoreMemory, auto seek to earliest position for rebuild
            self._logger.info('GlobalStoreMemory seek to earliest')
            assigned_partitions = list()
            last_offsets = dict()
            for i in range(0, self._nb_replica):
                assigned_partitions.append(TopicPartition(self._topic_store, i))
            for j in range(0, self._nb_replica):
                last_offsets[TopicPartition(self._topic_store, j)] = 0
            await self._global_store.set_store_position(self._current_instance, self._nb_replica, assigned_partitions,
                                                        last_offsets)
            try:
                await self._store_consumer.load_offsets('earliest')
            except (TopicPartitionError, NoPartitionAssigned) as err:
                self._logger.exception(f'{err.__str__()}')
                raise CanNotInitializeStore
        else:
            try:
                global_store_metadata = await self._global_store.get_metadata()
            except StoreKeyNotFound:
                # If metadata doesn't exist in DB
                assigned_partitions = list()
                last_offsets = dict()
                for i in range(0, self._nb_replica):
                    assigned_partitions.append(TopicPartition(self._topic_store, self._current_instance))
                for j in range(0, self._nb_replica):
                    last_offsets[TopicPartition(self._topic_store, self._current_instance)] = 0
                await self._global_store.set_store_position(self._current_instance, self._nb_replica,
                                                            assigned_partitions, last_offsets)
                try:
                    await self._store_consumer.load_offsets('earliest')
                except (TopicPartitionError, NoPartitionAssigned) as err:
                    self._logger.exception(f'{err.__str__()}')
                    raise CanNotInitializeStore
            else:
                # If metadata is exist in DB
                for tp, offset in global_store_metadata.last_offsets.items():
                    try:
                        await self._store_consumer.seek_custom(tp.topic, tp.partition, offset)
                    except (OffsetError, TopicPartitionError, NoPartitionAssigned) as err:
                        self._logger.exception(f'{err.__str__()}')
                        raise CanNotInitializeStore
                await self._global_store.set_store_position(self._current_instance, self._nb_replica,
                                                            global_store_metadata.assigned_partitions,
                                                            global_store_metadata.last_offsets)

    def set_local_store_initialize(self, initialized: bool) -> None:
        self._local_store.set_initialized(initialized)

    def set_global_store_initialize(self, initialized: bool) -> None:
        self._global_store.set_initialized(initialized)

    # Sugar functions for local store management
    async def set_from_local_store(self, key: str, value: bytes) -> RecordMetadata:
        """
        Set from local store

        Args:
            key (str): Object key as string
            value (bytes): Object value as bytes
        Returns:
            None
        """
        if self._local_store.is_initialized():
            store_builder = StoreRecord(key=key, ctype='set', value=value)
            try:
                record_metadata: RecordMetadata = await self._store_producer.send_and_await(store_builder,
                                                                                            self._topic_store)
            except (KeyErrorSendEvent, ValueErrorSendEvent, TypeErrorSendEvent, FailToSendEvent):
                raise FailToSendStoreRecord
            await self._local_store.update_metadata_tp_offset(TopicPartition(record_metadata.topic,
                                                                             record_metadata.partition),
                                                              record_metadata.offset)
            await self._local_store.set(key, value)
            return record_metadata
        else:
            raise UninitializedStore

    async def get_from_local_store(self, key: str) -> bytes:
        """
        Get from local store

        Args:
            key (str): Object key as string

        Returns:
            bytes: Object value as bytes
        """
        if self._local_store.is_initialized():
            return await self._local_store.get(key)
        raise UninitializedStore

    async def delete_from_local_store(self, key: str) -> RecordMetadata:
        """
        Delete from local store

        Args:
            key (str): Object key as string

        Returns:
            None
        """
        if self._local_store.is_initialized():
            store_builder = StoreRecord(key=key, ctype='del', value=b'')
            try:
                record_metadata: RecordMetadata = await self._store_producer.send_and_await(store_builder,
                                                                                            self._topic_store)
                await self._local_store.update_metadata_tp_offset(TopicPartition(record_metadata.topic,
                                                                                 record_metadata.partition),
                                                                  record_metadata.offset)
            except (KeyErrorSendEvent, ValueErrorSendEvent, TypeErrorSendEvent, FailToSendEvent):
                raise FailToSendStoreRecord
            await self._local_store.delete(key)
            return record_metadata
        else:
            raise UninitializedStore

    async def set_from_local_store_rebuild(self, key: str, value: bytes) -> None:
        await self._local_store.build_set(key, value)

    async def delete_from_local_store_rebuild(self, key: str) -> None:
        await self._local_store.build_delete(key)

    async def update_metadata_from_local_store(self, tp: TopicPartition, offset: int) -> None:
        """
        Sugar function, update local store metadata

        Args:
            tp (TopicPartition): Topic and partition
            offset (int): offset

        Returns:
            None
        """
        await self._local_store.update_metadata_tp_offset(tp, offset)

    # Sugar function for global store management
    async def get_from_global_store(self, key: str) -> bytes:
        """
        Sugar function, get from global store

        Args:
            key (str): Object key as string

        Returns:
            None
        """
        return await self._global_store.get(key)

    async def set_from_global_store(self, key: str, value: bytes) -> None:
        """
        Sugar function, set from global store

        Args:
            key (str): Object key as string
            value (bytes): Object values as bytes

        Returns:
            None
        """
        await self._global_store.global_set(key, value)

    async def delete_from_global_store(self, key: str) -> None:
        """
        Sugar function, delete from global store

        Args:
            key (str): Object key as string

        Returns:
            None
        """
        await self._global_store.global_delete(key)

    async def update_metadata_from_global_store(self, tp: TopicPartition, offset: int) -> None:
        """
        Sugar function, update global store metadata

        Args:
            tp (TopicPartition): Topic and partition
            offset (int): offset

        Returns:
            None
        """
        await self._global_store.update_metadata_tp_offset(tp, offset)

    # Get stores
    def get_local_store(self) -> BaseLocalStore:
        """
        Returns local store instance

        Returns:
            BaseLocalStore: Local store instance
        """
        return self._local_store

    def get_global_store(self) -> BaseGlobalStore:
        """
        Return global store instance

        Returns:
            BaseGlobalStore: Global store instance
        """
        return self._global_store

    # Get info
    def get_current_instance(self) -> int:
        """
        Returns current instance

        Returns:
            int: current instance as integer
        """
        return self._current_instance

    def get_nb_replica(self) -> int:
        """
        Returns nb replica

        Returns:
            int: current instance as integer
        """
        return self._nb_replica

    def is_event_sourcing(self) -> bool:
        """
        Returns if StoreBuilder as in event_sourcing mod

        Returns:
            bool: event_sourcing
        """
        return self._event_sourcing

    # Get producer & consumer
    def get_producer(self) -> KafkaProducer:
        """
        Returns StoreBuilder tongaProducer

        Returns:
            tongaProducer: tonga Producer
        """
        return self._store_producer

    def get_consumer(self) -> KafkaConsumer:
        """
        Returns StoreBuilder tongaConsumer

        Returns:
            tongaConsumer: tonga Consumer
        """
        return self._store_consumer
