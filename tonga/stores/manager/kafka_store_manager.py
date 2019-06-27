#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" StoreBuilder class

This class manage one local & global store.
"""

import asyncio
from asyncio import (AbstractEventLoop, Future)
from logging import (Logger, getLogger)
from typing import List, Union, Dict

from tonga.models.store.store_record import StoreRecord
from tonga.models.structs.positioning import (KafkaPositioning, BasePositioning)
from tonga.models.structs.store_record_type import StoreRecordType
from tonga.services.consumer.errors import (OffsetError, TopicPartitionError, NoPartitionAssigned)
from tonga.services.consumer.kafka_consumer import KafkaConsumer
from tonga.services.coordinator.client.kafka_client import KafkaClient
from tonga.services.coordinator.partitioner.statefulset_partitioner import StatefulsetPartitioner
from tonga.services.producer.errors import (KeyErrorSendEvent, ValueErrorSendEvent,
                                            TypeErrorSendEvent, FailToSendEvent)
from tonga.services.producer.kafka_producer import KafkaProducer
from tonga.services.serializer.base import BaseSerializer
from tonga.stores.errors import StoreKeyNotFound
from tonga.stores.global_store.base import BaseGlobalStore
from tonga.stores.global_store.memory import GlobalStoreMemory
from tonga.stores.local_store.base import BaseLocalStore
from tonga.stores.local_store.memory import LocalStoreMemory
from tonga.stores.manager.base import BaseStoreManager
from tonga.stores.manager.errors import (UninitializedStore, CanNotInitializeStore, FailToSendStoreRecord)
from tonga.stores.metadata.kafka_metadata import KafkaStoreMetaData

__all__ = [
    'KafkaStoreManager'
]


class KafkaStoreManager(BaseStoreManager):
    """Kafka Store Manager

    This class manage one local & global store. He builds stores on start of services.
    He has own KafkaProducer & KafkaConsumer
    """

    name: str
    _topic_store: str
    _rebuild: bool
    _event_sourcing: bool

    _local_store: Union[LocalStoreMemory]
    _global_store: Union[GlobalStoreMemory]

    _loop: AbstractEventLoop

    _store_consumer: KafkaConsumer
    _store_producer: KafkaProducer

    _logger: Logger

    _stores_partitions: List[BasePositioning]

    def __init__(self, name: str, client: KafkaClient, topic_store: str, serializer: BaseSerializer,
                 local_store: Union[LocalStoreMemory], global_store: Union[GlobalStoreMemory],
                 loop: AbstractEventLoop, rebuild: bool = False, event_sourcing: bool = False) -> None:
        """
        KafkaStoreManager constructor

        Args:
            name (str): KafkaStoreManager name used for client_id creation
            client (KafkaClient): Initialization class (contains, client_id / bootstraps_server)
            topic_store: Name topic where store event was send
            serializer: Serializer, this param was sends by tonga
            local_store: Local store instance Memory / Shelve / RockDB / ...
            global_store: Global store instance Memory / Shelve / RockDB / ...
            loop: Asyncio loop
            rebuild: If is true store is rebuild from first offset of topic / partition
            event_sourcing: If is true StateBuilder block instance for write in local & global store, storage will
                            be only updated by handle store function, more details in StorageBuilder.
                            Otherwise instance can only write in own local store, global store is only read only
        """
        self._client = client
        self.name = name

        self._rebuild = rebuild
        self._event_sourcing = event_sourcing

        self._serializer = serializer

        self._topic_store = topic_store

        self._local_store = local_store
        self._global_store = global_store

        self._logger = getLogger('tonga')
        self._loop = loop

        self._store_consumer = KafkaConsumer(client=client, serializer=self._serializer, auto_offset_reset='earliest',
                                             topics=[self._topic_store], group_id=f'{self.name}_consumer',
                                             client_id=f'{self.name}_producer_{self._client.cur_instance}',
                                             loop=self._loop, isolation_level='read_committed',
                                             assignors_data={'instance': self._client.cur_instance,
                                                             'nb_replica': self._client.nb_replica,
                                                             'assignor_policy': 'all'},
                                             store_builder=self)

        partitioner = StatefulsetPartitioner(instance=self._client.cur_instance)

        self._store_producer = KafkaProducer(client=self._client,
                                             client_id=f'{self.name}_producer_{self._client.cur_instance}',
                                             partitioner=partitioner,
                                             loop=self._loop, serializer=self._serializer, acks='all')

        self._stores_partitions = list()

    def return_consumer_task(self) -> Future:
        """ Return consumer task to ensure_future

        Returns:
            Future: return consumer future task
        """
        return asyncio.ensure_future(self._store_consumer.listen_store_records(self._rebuild), loop=self._loop)

    async def initialize_store_manager(self) -> None:
        """Initializes store manager, this function was call by consumer for init store manager and set StoreMetaData,
        seek to last committed offset if store_metadata exist.

        Raises:
            CanNotInitializeStore: raised was TopicPartitionError or NoPartitionAssigned was raised

        Returns:
            None
        """

        self._logger.info('Start initialize store manager')
        self._local_store.set_metadata_class(KafkaStoreMetaData)
        self._global_store.set_metadata_class(KafkaStoreMetaData)

        # Initialize local store
        if isinstance(self._local_store, LocalStoreMemory):
            # If _local_store is an instance from LocalStoreMemory, auto seek to earliest position for rebuild
            self._logger.info('LocalStoreMemory seek to earliest')
            assigned_partitions_mem: List[BasePositioning] = list()
            last_offsets_mem: Dict[str, BasePositioning] = dict()

            assigned_partitions_mem.append(KafkaPositioning(self._topic_store, self._client.cur_instance, 0))
            last_offsets_mem[KafkaPositioning.make_class_assignment_key(self._topic_store,
                                                                        self._client.cur_instance)] = \
                KafkaPositioning(self._topic_store, self._client.cur_instance, 0)

            await self._local_store.set_store_position(KafkaStoreMetaData(assigned_partitions=assigned_partitions_mem,
                                                                          last_offsets=last_offsets_mem,
                                                                          current_instance=self._client.cur_instance,
                                                                          nb_replica=self._client.nb_replica))
            try:
                await self._store_consumer.load_offsets('earliest')
            except (TopicPartitionError, NoPartitionAssigned) as err:
                self._logger.exception('%s', err.__str__())
                raise CanNotInitializeStore
        else:
            try:
                # Try to get local_store_metadata, seek at last read offset
                local_store_metadata = await self._local_store.get_metadata()
            except StoreKeyNotFound:
                # If metadata doesn't exist in DB, auto seek to earliest position for rebuild
                assigned_partitions_cls: List[BasePositioning] = list()
                last_offsets_cls: Dict[str, BasePositioning] = dict()

                assigned_partitions_cls.append(KafkaPositioning(self._topic_store, self._client.cur_instance, 0))
                key = KafkaPositioning.make_class_assignment_key(self._topic_store, self._client.cur_instance)
                last_offsets_cls[key] = KafkaPositioning(self._topic_store, self._client.cur_instance, 0)

                await self._local_store.set_store_position(
                    KafkaStoreMetaData(assigned_partitions=assigned_partitions_cls,
                                       last_offsets=last_offsets_cls,
                                       current_instance=self._client.cur_instance,
                                       nb_replica=self._client.nb_replica))
                try:
                    await self._store_consumer.load_offsets('earliest')
                except (TopicPartitionError, NoPartitionAssigned) as err:
                    self._logger.exception('%s', err.__str__())
                    raise CanNotInitializeStore
            else:
                # If metadata is exist in DB, , auto seek to last position
                try:
                    last_kafka_positioning = local_store_metadata.last_offsets[
                        KafkaPositioning.make_class_assignment_key(self._topic_store, self._client.cur_instance)]

                    await self._store_consumer.seek_custom(last_kafka_positioning)

                    await self._local_store.set_store_position(
                        KafkaStoreMetaData(assigned_partitions=local_store_metadata.assigned_partitions,
                                           last_offsets=local_store_metadata.last_offsets,
                                           current_instance=self._client.cur_instance,
                                           nb_replica=self._client.nb_replica))
                except (OffsetError, TopicPartitionError, NoPartitionAssigned) as err:
                    self._logger.exception('%s', err.__str__())
                    raise CanNotInitializeStore

        # Initialize global store
        if isinstance(self._global_store, GlobalStoreMemory):
            # If _global_store is an instance from GlobalStoreMemory, auto seek to earliest position for rebuild
            g_assigned_partitions_mem: List[BasePositioning] = list()
            g_last_offsets_mem: Dict[str, BasePositioning] = dict()
            self._logger.info('GlobalStoreMemory seek to earliest')
            for i in range(0, self._client.nb_replica):
                if i != self._client.cur_instance:
                    g_assigned_partitions_mem.append(KafkaPositioning(self._topic_store, i, 0))
            for j in range(0, self._client.nb_replica):
                if j != self._client.cur_instance:
                    g_last_offsets_mem[KafkaPositioning.make_class_assignment_key(self._topic_store, j)] = \
                        KafkaPositioning(self._topic_store, j, 0)

            await self._global_store.set_store_position(
                KafkaStoreMetaData(assigned_partitions=g_assigned_partitions_mem,
                                   last_offsets=g_last_offsets_mem,
                                   current_instance=self._client.cur_instance,
                                   nb_replica=self._client.nb_replica))
            try:
                await self._store_consumer.load_offsets('earliest')
            except (TopicPartitionError, NoPartitionAssigned) as err:
                self._logger.exception('%s', err.__str__())
                raise CanNotInitializeStore
        else:
            try:
                global_store_metadata = await self._global_store.get_metadata()
            except StoreKeyNotFound:
                # If metadata doesn't exist in DB
                g_assigned_partitions_cls: List[BasePositioning] = list()
                g_last_offsets_cls: Dict[str, BasePositioning] = dict()

                for i in range(0, self._client.nb_replica):
                    if i != self._client.cur_instance:
                        g_assigned_partitions_cls.append(KafkaPositioning(self._topic_store, i, 0))
                for j in range(0, self._client.nb_replica):
                    if j != self._client.cur_instance:
                        g_last_offsets_cls[KafkaPositioning.make_class_assignment_key(self._topic_store, j)] = \
                            KafkaPositioning(self._topic_store, j, 0)

                await self._global_store.set_store_position(
                    KafkaStoreMetaData(assigned_partitions=g_assigned_partitions_cls,
                                       last_offsets=g_last_offsets_cls,
                                       current_instance=self._client.cur_instance,
                                       nb_replica=self._client.nb_replica))
                try:
                    await self._store_consumer.load_offsets('earliest')
                except (TopicPartitionError, NoPartitionAssigned) as err:
                    self._logger.exception('%s', err.__str__())
                    raise CanNotInitializeStore
            else:
                # If metadata is exist in DB
                for key, kafka_positioning in global_store_metadata.last_offsets.items():
                    try:
                        await self._store_consumer.seek_custom(kafka_positioning)
                    except (OffsetError, TopicPartitionError, NoPartitionAssigned) as err:
                        self._logger.exception('%s', err.__str__())
                        raise CanNotInitializeStore

                await self._global_store.set_store_position(
                    KafkaStoreMetaData(assigned_partitions=global_store_metadata.assigned_partitions,
                                       last_offsets=global_store_metadata.last_offsets,
                                       current_instance=self._client.cur_instance,
                                       nb_replica=self._client.nb_replica))

    def set_local_store_initialize(self, initialized: bool) -> None:
        """Set local store initialized flag

        Args:
            initialized (bool): true for set local store initialized, otherwise false

        Returns:
            None
        """
        self._local_store.set_initialized(initialized)

    def set_global_store_initialize(self, initialized: bool) -> None:
        """Set global store initialized flag

        Args:
            initialized (bool): true for set global store initialized, otherwise false

        Returns:
            None
        """
        self._global_store.set_initialized(initialized)

    # Sugar functions for local store management
    async def set_from_local_store(self, key: str, value: bytes) -> None:
        """Set from local store

        Args:
            key (str): Object key as string
            value (bytes): Object value as bytes

        Raises:
            FailToSendStoreRecord: Raised when store builder fail to send StoreRecord
            UninitializedStore: Raised when initialize store flag is not true

        Returns:
            RecordMetadata: Kafka record metadata (see kafka-python for more details)
        """
        while not self._local_store.is_initialized():
            await asyncio.sleep(2)

        self._logger.debug('self._local_store.is_initialized() = %s', self._local_store.is_initialized())

        if self._local_store.is_initialized():
            store_builder = StoreRecord(key=key, operation_type=StoreRecordType('set'), value=value)

            try:
                record_metadata: BasePositioning = await self._store_producer.send_and_wait(store_builder,
                                                                                            self._topic_store)
            except (KeyErrorSendEvent, ValueErrorSendEvent, TypeErrorSendEvent, FailToSendEvent):
                raise FailToSendStoreRecord
            await self._local_store.update_metadata_tp_offset(KafkaPositioning(record_metadata.get_topics(),
                                                                               record_metadata.get_partition(),
                                                                               record_metadata.get_current_offset()))
            await self._local_store.set(key, value)
        else:
            raise UninitializedStore

    async def get_from_local_store(self, key: str) -> bytes:
        """Get from local store

        Args:
            key (str): Object key as string

        Raises:
            UninitializedStore: Raised when initialize store flag is not true

        Returns:
            bytes: Object value as bytes
        """
        while not self._local_store.is_initialized():
            await asyncio.sleep(2)

        if self._local_store.is_initialized():
            return await self._local_store.get(key)
        raise UninitializedStore

    async def delete_from_local_store(self, key: str) -> None:
        """Delete from local store

        Args:
            key (str): Object key as string

        Raises:
            FailToSendStoreRecord: Raised when store builder fail to send StoreRecord
            UninitializedStore: Raised when initialize store flag is not true

        Returns:
            RecordMetadata: Kafka record metadata (see kafka-python for more details)
        """
        while not self._local_store.is_initialized():
            await asyncio.sleep(2)

        if self._local_store.is_initialized():
            store_builder = StoreRecord(key=key, operation_type=StoreRecordType('del'), value=b'')
            try:
                record_metadata: BasePositioning = await self._store_producer.send_and_wait(store_builder,
                                                                                            self._topic_store)
            except (KeyErrorSendEvent, ValueErrorSendEvent, TypeErrorSendEvent, FailToSendEvent):
                raise FailToSendStoreRecord

            await self._local_store.update_metadata_tp_offset(KafkaPositioning(record_metadata.get_topics(),
                                                                               record_metadata.get_partition(),
                                                                               record_metadata.get_current_offset()))
            await self._local_store.delete(key)
        else:
            raise UninitializedStore

    async def set_from_local_store_rebuild(self, key: str, value: bytes) -> None:
        """ Set key & value in local store in rebuild mod

        Args:
            key (str): Key value as string
            value (bytes): Value as bytes

        Returns:
            None
        """
        await self._local_store.build_set(key, value)

    async def delete_from_local_store_rebuild(self, key: str) -> None:
        """ Delete value by key in local store in rebuild mod

        Args:
            key (str): Key value as string

        Returns:
            None
        """
        await self._local_store.build_delete(key)

    async def update_metadata_from_local_store(self, positioning: BasePositioning) -> None:
        """ Update local store metadata

        Args:
            positioning (BasePositioning): KafkaPositioning class

        Returns:
            None
        """
        await self._local_store.update_metadata_tp_offset(positioning)

    # Sugar function for global store management
    async def get_from_global_store(self, key: str) -> bytes:
        """ Get value by key in global store

        Args:
            key (str): Key value as string

        Returns:
            bytes: return value as bytes
        """
        return await self._global_store.get(key)

    async def set_from_global_store(self, key: str, value: bytes) -> None:
        """ Set key & value in global store

        Warnings:
            DO NOT USE THIS FUNCTION, IT ONLY CALLED BY CONSUMER FOR GLOBAL STORE CONSTRUCTION

        Args:
            key (str): Key value as string
            value (bytes): Value as bytes

        Returns:
            None
        """
        await self._global_store.global_set(key, value)

    async def delete_from_global_store(self, key: str) -> None:
        """ Delete key & value in global store

        Warnings:
            DO NOT USE THIS FUNCTION, IT ONLY CALLED BY CONSUMER FOR GLOBAL STORE CONSTRUCTION

        Args:
            key (str): Key value as string

        Returns:
            None
        """
        await self._global_store.global_delete(key)

    async def update_metadata_from_global_store(self, positioning: BasePositioning) -> None:
        """ Update global store metadata

        Args:
            positioning (BasePositioning): KafkaPositioning class

        Returns:
            None
        """
        await self._global_store.update_metadata_tp_offset(positioning)

    # Get stores
    def get_local_store(self) -> BaseLocalStore:
        """Returns local store instance

        Returns:
            BaseLocalStore: Local store instance
        """
        return self._local_store

    def get_global_store(self) -> BaseGlobalStore:
        """Return global store instance

        Returns:
            BaseGlobalStore: Global store instance
        """
        return self._global_store

    # Get info
    def get_current_instance(self) -> int:
        """ Returns current instance

        Returns:
            int: current instance as integer
        """
        return self._client.cur_instance

    def get_nb_replica(self) -> int:
        """ Returns nb replica

        Returns:
            int: current instance as integer
        """
        return self._client.nb_replica

    def is_event_sourcing(self) -> bool:
        """ Returns if StoreBuilder as in event_sourcing mod

        Returns:
            bool: event_sourcing
        """
        return self._event_sourcing

    # Get producer & consumer
    def get_producer(self) -> KafkaProducer:
        """ Returns StoreBuilder tongaProducer

        Returns:
            tongaProducer: tonga Producer
        """
        return self._store_producer

    def get_consumer(self) -> KafkaConsumer:
        """ Returns StoreBuilder tongaConsumer

        Returns:
            tongaConsumer: tonga Consumer
        """
        return self._store_consumer
