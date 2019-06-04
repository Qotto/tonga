#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import logging
import asyncio
from asyncio import AbstractEventLoop
from logging import Logger
from aiokafka import TopicPartition
from aiokafka.producer.message_accumulator import RecordMetadata
from aiokafka.producer import AIOKafkaProducer
from aiokafka.producer.message_accumulator import BatchBuilder
from aiokafka.errors import KafkaError, KafkaTimeoutError
from aiokafka.producer.producer import TransactionContext

from typing import Union, List, Dict, Type

# BaseSerializer / KafkaKeySerializer import
from tonga.services.serializer.base import BaseSerializer
from tonga.services.serializer.kafka_key import KafkaKeySerializer

# BasePartitioner import
from tonga.services.coordinator.partitioner.base import BasePartitioner

# Base Producer import
from tonga.services.producer.base import BaseProducer

# Model import
from tonga.models.events.base import BaseModel
from tonga.models.store_record.base import BaseStoreRecord

# Exception import
from tonga.services.errors import BadSerializer
from tonga.services.producer.errors import (ProducerConnectionError, AioKafkaProducerBadParams,
                                               KafkaProducerError, KafkaProducerTimeoutError, KeyErrorSendEvent,
                                               ValueErrorSendEvent, TypeErrorSendEvent, FailToSendEvent,
                                               UnknownEventBase, FailToSendBatch, KafkaProducerNotStartedError,
                                               KafkaProducerAlreadyStartedError)

__all__ = [
    'KafkaProducer',
]


class KafkaProducer(BaseProducer):
    """
    KafkaProducer Class, this class make bridge between AioKafkaProducer an tonga

    Attributes:
        name (str): Kafka Producer name
        logger (Logger): Python logger
        serializer (BaseSerializer): Serializer encode & decode event
        _bootstrap_servers (Union[str, List[str]): ‘host[:port]’ string (or list of ‘host[:port]’ strings) that
                                                    the consumer should contact to bootstrap initial cluster metadata
        _client_id (str): A name for this client. This string is passed in each request to servers and can be used
                          to identify specific server-side log entries that correspond to this client
        _acks (Union[int, str]): The number of acknowledgments the producer requires the leader to have
                                 received before considering a request complete. Possible value (0 / 1 / all)
        _running (bool): Is running flag
        _transactional_id (str): Id for make transactional process
        _kafka_producer (AIOKafkaProducer): AioKafkaProducer for more information go to
        _loop (AbstractEventLoop): Asyncio loop
    """
    name: str
    logger: Logger
    serializer: BaseSerializer
    _bootstrap_servers: Union[str, List[str]]
    _client_id: str
    _acks: Union[int, str]
    _running: bool
    _transactional_id: str
    _kafka_producer: AIOKafkaProducer
    _loop: AbstractEventLoop

    def __init__(self, name: str, bootstrap_servers: Union[str, List[str]], client_id: str, serializer: BaseSerializer,
                 loop: AbstractEventLoop, partitioner: BasePartitioner, acks: Union[int, str] = 1,
                 transactional_id: str = None) -> None:
        """
        KafkaProducer constructor

        Args:
            name (str): Kafka Producer name
            bootstrap_servers (Union[str, List[str]): ‘host[:port]’ string (or list of ‘host[:port]’ strings) that
                                                    the consumer should contact to bootstrap initial cluster metadata
            client_id (str): A name for this client. This string is passed in each request to servers and can be used
                            to identify specific server-side log entries that correspond to this client
            serializer (BaseSerializer): Serializer encode & decode event
            acks (Union[int, str]): The number of acknowledgments the producer requires the leader to have
                                 received before considering a request complete. Possible value (0 / 1 / all)
            transactional_id: Id for make transactional process

        Returns:
            None
        """
        super().__init__()
        self.name = name
        self.logger = logging.getLogger('tonga')

        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._acks = acks
        if isinstance(serializer, BaseSerializer):
            self.serializer = serializer
        else:
            raise BadSerializer
        self._transactional_id = transactional_id
        self._running = False
        self._loop = loop

        try:
            self._kafka_producer = AIOKafkaProducer(loop=self._loop, bootstrap_servers=self._bootstrap_servers,
                                                    client_id=self._client_id, acks=self._acks,
                                                    value_serializer=self.serializer.encode,
                                                    transactional_id=self._transactional_id,
                                                    key_serializer=KafkaKeySerializer.encode,
                                                    partitioner=partitioner)
        except ValueError as err:
            self.logger.exception(f'{err.__str__()}')
            raise AioKafkaProducerBadParams
        except KafkaError as err:
            self.logger.exception(f'{err.__str__()}')
            raise KafkaProducerError
        self.logger.debug(f'Create new producer {client_id}')

    async def start_producer(self) -> None:
        """
        Start producer

        Returns:
            None
        """
        if self._running:
            raise KafkaProducerAlreadyStartedError
        for retry in range(2):
            try:
                await self._kafka_producer.start()
                self._running = True
                self.logger.debug(f'Start producer : {self._client_id}')
            except KafkaTimeoutError as err:
                self.logger.exception(f'{err.__str__()}')
                await asyncio.sleep(1)
            except ConnectionError as err:
                self.logger.exception(f'{err.__str__()}')
                await asyncio.sleep(1)
            except KafkaError as err:
                self.logger.exception(f'{err.__str__()}')
                raise err
            else:
                break
        else:
            raise ProducerConnectionError

    async def stop_producer(self) -> None:
        """
        Stop producer

        Returns:
            None
        """
        if not self._running:
            raise KafkaProducerNotStartedError
        try:
            await self._kafka_producer.stop()
            self._running = False
            self.logger.debug(f'Stop producer : {self._client_id}')
        except KafkaTimeoutError as err:
            self.logger.exception(f'{err.__str__()}')
            raise KafkaProducerTimeoutError
        except KafkaError as err:
            self.logger.exception(f'{err.__str__()}')
            raise err

    def is_running(self) -> bool:
        return self._running

    # Transaction sugar function
    def init_transaction(self) -> TransactionContext:
        """
        Sugar function, inits transaction

        Returns:
            TransactionContext: Aiokafka TransactionContext
        """
        return self._kafka_producer.transaction()

    async def end_transaction(self, committed_offsets: Dict[TopicPartition, int], group_id: str) -> None:
        """
        Sugar function, ends transaction

        Args:
            committed_offsets (Dict[TopicPartition, int]): Committed offsets during transaction
            group_id (str): Group_id to commit

        Returns:
            None
        """
        await self._kafka_producer.send_offsets_to_transaction(committed_offsets, group_id)

    async def send_and_await(self, event: Union[BaseModel, BaseStoreRecord], topic: str) -> Union[RecordMetadata, None]:
        """
        This function send a massage and await an acknowledgments

        Args:
            event (BaseModel): Event to send in Kafka, inherit form BaseModel
            topic (str): Topic name to send massage

        Raises:
            ...
            # TODO write missing raises docstring

        Returns:
            None
        """
        if not self._running:
            await self.start_producer()

        record_metadata = None
        for retry in range(4):
            try:
                self.logger.debug(f'Send event {event.event_name()}')
                if isinstance(event, BaseModel):
                    record_metadata = await self._kafka_producer.send_and_wait(topic=topic, value=event,
                                                                               key=event.partition_key)
                elif isinstance(event, BaseStoreRecord):
                    record_metadata = await self._kafka_producer.send_and_wait(topic=topic, value=event,
                                                                               key=event.key)
                else:
                    raise UnknownEventBase
            except KafkaTimeoutError as err:
                self.logger.exception(f'{err.__str__()}')
                await asyncio.sleep(1)
            except KeyError as err:
                self.logger.exception(f'{err.__str__()}')
                raise KeyErrorSendEvent
            except ValueError as err:
                self.logger.exception(f'{err.__str__()}')
                raise ValueErrorSendEvent
            except TypeError as err:
                self.logger.exception(f'{err.__str__()}')
                raise TypeErrorSendEvent
            except KafkaError as err:
                self.logger.exception(f'{err.__str__()}')
                raise err
            else:
                break
        else:
            raise FailToSendEvent
        return record_metadata

    async def send(self, event: BaseModel, topic: str) -> Union[RecordMetadata, None]:
        if not self._running:
            await self.start_producer()

        record_metadata = None
        for retry in range(4):
            try:
                self.logger.debug(f'Send event {event.event_name()}')
                if isinstance(event, BaseModel):
                    record_metadata = await self._kafka_producer.send(topic=topic, value=event,
                                                                      key=event.partition_key)
                elif isinstance(event, BaseStoreRecord):
                    record_metadata = await self._kafka_producer.send(topic=topic, value=event,
                                                                      key=event.key)
                else:
                    raise UnknownEventBase
            except KafkaTimeoutError as err:
                self.logger.exception(f'{err.__str__()}')
                await asyncio.sleep(1)
            except KeyError as err:
                self.logger.exception(f'{err.__str__()}')
                raise KeyErrorSendEvent
            except ValueError as err:
                self.logger.exception(f'{err.__str__()}')
                raise ValueErrorSendEvent
            except TypeError as err:
                self.logger.exception(f'{err.__str__()}')
                raise TypeErrorSendEvent
            except KafkaError as err:
                self.logger.exception(f'{err.__str__()}')
                raise err
            else:
                break
        else:
            raise FailToSendEvent
        return record_metadata

    async def create_batch(self) -> BatchBuilder:
        if not self._running:
            await self.start_producer()
        self.logger.debug(f'Create batch')
        return self._kafka_producer.create_batch()

    async def send_batch(self, batch: BatchBuilder, topic: str, partition: int = 0) -> None:
        if not self._running:
            await self.start_producer()

        for retry in range(4):
            try:
                self.logger.debug(f'Send batch')
                await self._kafka_producer.send_batch(batch=batch, topic=topic, partition=partition)
            except KafkaTimeoutError as err:
                self.logger.exception(f'{err.__str__()}')
                await asyncio.sleep(1)
            except KeyError as err:
                self.logger.exception(f'{err.__str__()}')
                raise KeyErrorSendEvent
            except ValueError as err:
                self.logger.exception(f'{err.__str__()}')
                raise ValueErrorSendEvent
            except TypeError as err:
                self.logger.exception(f'{err.__str__()}')
                raise TypeErrorSendEvent
            except KafkaError as err:
                self.logger.exception(f'{err.__str__()}')
                raise err
            else:
                break
        else:
            raise FailToSendBatch

    async def partitions_by_topic(self, topic: str) -> List[str]:
        if not self._running:
            await self.start_producer()
        try:
            self.logger.debug(f'Get partitions by topic')
            partitions = await self._kafka_producer.partitions_for(topic)
        except KafkaTimeoutError as err:
            self.logger.exception(f'{err.__str__()}')
            raise KafkaProducerTimeoutError
        except KafkaError as err:
            self.logger.exception(f'{err.__str__()}')
            raise err
        return partitions

    def get_kafka_producer(self) -> AIOKafkaProducer:
        return self._kafka_producer
