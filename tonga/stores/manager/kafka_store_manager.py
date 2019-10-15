#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" KafkaStoreManager class

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
from tonga.services.serializer.avro import AvroSerializer
from tonga.models.structs.persistency_type import PersistencyType
from tonga.stores.errors import StoreKeyNotFound
from tonga.stores.persistency.memory import MemoryPersistency
from tonga.stores.local_store import LocalStore
from tonga.stores.global_store import GlobalStore
from tonga.stores.manager.base import BaseStoreManager
from tonga.stores.manager.errors import (UninitializedStore, CanNotInitializeStore, FailToSendStoreRecord)

__all__ = [
    'KafkaStoreManager'
]


class KafkaStoreManager(BaseStoreManager):
    """Kafka Store Manager

    This class manage one local & global store. He builds stores on services start. He has own KafkaProducer
    & KafkaConsumer
    """
    _topic_store: str

    def __init__(self, client: KafkaClient, topic_store: str, persistency_type: PersistencyType,
                 serializer: AvroSerializer, loop: AbstractEventLoop, rebuild: bool = False) -> None:
        """
        KafkaStoreManager constructor

        Args:
            client (KafkaClient): Initialization class (contains, client_id / bootstraps_server)
            topic_store (str): Name topic where store event was send
            loop (AbstractEventLoop): Asyncio loop
            rebuild (bool): If is true store is rebuild from first offset of topic / partition
        """
        self._topic_store = topic_store
        self._persistency_type = persistency_type

        self._client = client
        self._loop = loop
        self._rebuild = rebuild

        self._local_store = LocalStore(self._persistency_type, self._loop)
        self._global_store = GlobalStore(self._persistency_type)

        self._serializer = serializer

        client_id = f'{self._client.client_id}-store-consumer-{self._client.cur_instance}'

        self._store_consumer = KafkaConsumer(client=self._client, serializer=self._serializer,
                                             topics=[self._topic_store], loop=self._loop,
                                             group_id=client_id, client_id=client_id, isolation_level='read_committed',
                                             auto_offset_reset='earliest',
                                             assignors_data={'instance': self._client.cur_instance,
                                                             'nb_replica': self._client.nb_replica,
                                                             'assignor_policy': 'all'},
                                             store_manager=self)

        asyncio.ensure_future(self._store_consumer.listen_store_records(self._rebuild), loop=self._loop)

        self._store_producer = KafkaProducer(client=self._client,
                                             client_id=client_id,
                                             partitioner=StatefulsetPartitioner(instance=self._client.cur_instance),
                                             loop=self._loop, serializer=self._serializer, acks='all')

    def get_topic_store(self) -> str:
        return self._topic_store

    async def _initialize_stores(self) -> None:
        """ This method initialize stores (construct, pre-build)

        Abstract method

        Returns:
            None
        """
        self._logger.info('Start initialize store manager')

        # LocalStore part
        if isinstance(self._local_store.get_persistency(), MemoryPersistency):
            try:
                self._logger.info('LocalStore is an memory persistency, seek to earliest')
                await self._store_consumer.seek_to_beginning(KafkaPositioning(topic=self._topic_store,
                                                                              partition=self._client.cur_instance,
                                                                              current_offset=0))
            except (TopicPartitionError, NoPartitionAssigned) as err:
                self._logger.exception('%s', err.__str__())
                raise CanNotInitializeStore
        else:
            if not self._rebuild:
                try:
                    positioning = await self._store_consumer.get_last_committed_offsets()
                    key = KafkaPositioning.make_class_assignment_key(self._topic_store, self._client.cur_instance)
                    last_committed = positioning[key].get_current_offset()
                    if last_committed is None:
                        self._logger.info('LocalStore seek to beginning, %s, %s', self._topic_store,
                                          self._client.cur_instance)
                        await self._store_consumer.seek_to_beginning(KafkaPositioning(topic=self._topic_store,
                                                                                      partition=self._client.cur_instance,
                                                                                      current_offset=0))
                    else:

                        self._logger.info('LocalStore seek to last committed, %s, %s', self._topic_store,
                                          self._client.cur_instance)
                        await self._store_consumer.seek_to_last_commit(KafkaPositioning(topic=self._topic_store,
                                                                                        partition=self._client.cur_instance,
                                                                                        current_offset=last_committed))
                except (TopicPartitionError, NoPartitionAssigned) as err:
                    self._logger.exception('%s', err.__str__())
                    raise CanNotInitializeStore
            else:
                try:
                    # TODO flush db
                    self._logger.info('Rebuild flag is true LocalStore seek to earliest')
                    await self._store_consumer.seek_to_beginning(KafkaPositioning(topic=self._topic_store,
                                                                                  partition=self._client.cur_instance,
                                                                                  current_offset=0))
                except (TopicPartitionError, NoPartitionAssigned) as err:
                    self._logger.exception('%s', err.__str__())
                    raise CanNotInitializeStore

        # GlobalStore part
        if isinstance(self._local_store.get_persistency(), MemoryPersistency):
            for part in range(0, self._client.nb_replica):
                if part != self._client.cur_instance:
                    try:
                        self._logger.info('GlobalStore is an memory persistency, seek to earliest')
                        await self._store_consumer.seek_to_beginning(KafkaPositioning(topic=self._topic_store,
                                                                                      partition=part,
                                                                                      current_offset=0))
                    except (TopicPartitionError, NoPartitionAssigned) as err:
                        self._logger.exception('%s', err.__str__())
                        raise CanNotInitializeStore
        else:
            if not self._rebuild:
                for part in range(0, self._client.nb_replica):
                    if part != self._client.cur_instance:
                        try:
                            positioning = await self._store_consumer.get_last_committed_offsets()
                            key = KafkaPositioning.make_class_assignment_key(self._topic_store,
                                                                             self._client.cur_instance)
                            last_committed = positioning[key].get_current_offset()
                            if last_committed is None:
                                self._logger.info('LocalStore seek to beginning, %s, %s', self._topic_store,
                                                  part)
                                await self._store_consumer.seek_to_beginning(KafkaPositioning(topic=self._topic_store,
                                                                                              partition=part,
                                                                                              current_offset=0))
                            else:
                                self._logger.info('LocalStore seek to last committed, %s, %s', self._topic_store,
                                                  part)
                                await self._store_consumer.seek_to_last_commit(KafkaPositioning(topic=self._topic_store,
                                                                                                partition=part,
                                                                                                current_offset=last_committed))
                        except (TopicPartitionError, NoPartitionAssigned) as err:
                            self._logger.exception('%s', err.__str__())
                            raise CanNotInitializeStore
            else:
                # TODO flush db
                self._logger.info('Rebuild flag is true GlobalStore seek to earliest')
                for part in range(0, self._client.nb_replica):
                    if part != self._client.cur_instance:
                        try:
                            await self._store_consumer.seek_to_beginning(KafkaPositioning(topic=self._topic_store,
                                                                                          partition=part,
                                                                                          current_offset=0))
                        except (TopicPartitionError, NoPartitionAssigned) as err:
                            self._logger.exception('%s', err.__str__())
                            raise CanNotInitializeStore

    # Store function
    async def set_entry_in_local_store(self, key: str, value: bytes) -> None:
        """ Set an entry in local store

        This method send an StoreRecord in event bus and store entry asynchronously

        Args:
            key (str): Key entry as string
            value (bytes): Value as bytes

        Returns:
            None
        """
        if self._local_store.get_persistency().is_initialize():
            store_record = StoreRecord(key=key, value=value, operation_type=StoreRecordType('set'))
            try:
                record_metadata: BasePositioning = await self._store_producer.send_and_wait(store_record,
                                                                                            self._topic_store)
                asyncio.ensure_future(self._store_consumer.__getattribute__('_make_manual_commit').
                                      __call__([record_metadata]), loop=self._loop)
            except (KeyErrorSendEvent, ValueErrorSendEvent, TypeErrorSendEvent, FailToSendEvent):
                raise FailToSendStoreRecord
            await self._local_store.set(key, value)
        else:
            raise UninitializedStore

    async def get_entry_in_local_store(self, key: str) -> bytes:
        """ Get an entry by key in local store

        This method try to get an entry in local store asynchronously

        Args:
            key (str): Key entry as string

        Returns:
            bytes: return value as bytes
        """
        return await self._local_store.get(key)

    async def delete_entry_in_local(self, key: str) -> None:
        """ Delete an entry in local store

        This method send an StoreRecord in event bus and delete entry asynchronously

        Args:
            key (str): Key entry as string

        Returns:
            None
        """
        if self._local_store.get_persistency().is_initialize():
            store_record = StoreRecord(key=key, value=b'', operation_type=StoreRecordType('del'))
            try:
                record_metadata: BasePositioning = await self._store_producer.send_and_wait(store_record,
                                                                                            self._topic_store)
                asyncio.ensure_future(self._store_consumer.__getattribute__('_make_manual_commit').
                                      __call__([record_metadata]), loop=self._loop)
            except (KeyErrorSendEvent, ValueErrorSendEvent, TypeErrorSendEvent, FailToSendEvent):
                raise FailToSendStoreRecord
            await self._local_store.delete(key)
        else:
            raise UninitializedStore

    async def get_entry_in_global_store(self, key: str) -> bytes:
        """ Get an entry by key in global store

        This method try to get an entry in global store asynchronously

        Args:
            key (str): Key entry as string

        Returns:
            bytes: return value as bytes
        """
        return await self._global_store.get(key)

    # Storage builder part
    async def _build_set_entry_in_global_store(self, key: str, value: str) -> None:
        """ Set an entry in global store

        This protected method store an entry asynchronously

        Args:
            key (str): Key entry as string
            value (bytes): Value as bytes

        Returns:
            None
        """
        await self._global_store.__getattribute__('_build_set').__call__(key, value)

    async def _build_delete_entry_in_global_store(self, key: str) -> None:
        """ Delete an entry in global store

        This method delete an entry asynchronously

        Args:
            key (str): Key entry as string

        Returns:
            None
        """
        await self._global_store.__getattribute__('_build_delete').__call__(key)

    async def _build_set_entry_in_local_store(self, key: str, value: bytes) -> None:
        """ Set an entry in local store

        This protected method store an entry asynchronously

        Abstract method

        Args:
            key (str): Key entry as string
            value (bytes): Value as bytes

        Returns:
            None
        """
        await self._local_store.__getattribute__('_build_set').__call__(key, value)

    async def _build_delete_entry_in_local_store(self, key: str) -> None:
        """ Delete an entry in local store

        This method delete an entry asynchronously

        Abstract method

        Args:
            key (str): Key entry as string

        Returns:
            None
        """
        await self._local_store.__getattribute__('_build_delete').__call__(key)
