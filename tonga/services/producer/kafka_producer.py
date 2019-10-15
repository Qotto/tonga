#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

"""KafkaProducer class

Produce messages and send them in a Kafka topic.
"""

import asyncio
from logging import (getLogger, Logger)
from typing import Union, List, Dict, Awaitable

from aiokafka.errors import KafkaError, KafkaTimeoutError
from aiokafka.producer import AIOKafkaProducer
from aiokafka.producer.message_accumulator import BatchBuilder
from aiokafka.producer.producer import TransactionContext

from tonga.models.records.base import BaseRecord
from tonga.models.store.store_record import StoreRecord
from tonga.models.structs.positioning import (BasePositioning, KafkaPositioning)
from tonga.services.coordinator.client.kafka_client import KafkaClient
from tonga.services.coordinator.partitioner.base import BasePartitioner
from tonga.services.errors import BadSerializer
from tonga.services.producer.base import BaseProducer
from tonga.services.producer.errors import AioKafkaProducerBadParams
from tonga.services.producer.errors import FailToSendBatch
from tonga.services.producer.errors import FailToSendEvent
from tonga.services.producer.errors import KafkaProducerAlreadyStartedError
from tonga.services.producer.errors import KafkaProducerError
from tonga.services.producer.errors import KafkaProducerNotStartedError
from tonga.services.producer.errors import KafkaProducerTimeoutError
from tonga.services.producer.errors import KeyErrorSendEvent
from tonga.services.producer.errors import ProducerConnectionError
from tonga.services.producer.errors import TypeErrorSendEvent
from tonga.services.producer.errors import UnknownEventBase
from tonga.services.producer.errors import ValueErrorSendEvent
from tonga.services.serializer.base import BaseSerializer
from tonga.services.serializer.kafka_key import KafkaKeySerializer

__all__ = [
    'KafkaProducer',
]


class KafkaProducer(BaseProducer):
    """
    KafkaProducer Class, this class make bridge between AioKafkaProducer an tonga

    Attributes:
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
    logger: Logger
    serializer: BaseSerializer
    _client: KafkaClient
    _bootstrap_servers: Union[str, List[str]]
    _client_id: str
    _acks: Union[int, str]
    _running: bool
    _transactional_id: str
    _kafka_producer: AIOKafkaProducer
    _loop: asyncio.AbstractEventLoop

    def __init__(self, client: KafkaClient, serializer: BaseSerializer, loop: asyncio.AbstractEventLoop,
                 partitioner: BasePartitioner, client_id: str = None, acks: Union[int, str] = 1,
                 transactional_id: str = None) -> None:
        """
        KafkaProducer constructor

        Args:
            client (KafkaClient): Initialization class (contains, client_id / bootstraps_server)
            serializer (BaseSerializer): Serializer encode & decode event
            acks (Union[int, str]): The number of acknowledgments the producer requires the leader to have
                                 received before considering a request complete. Possible value (0 / 1 / all)
            client_id (str): Client name (if is none, KafkaConsumer use KafkaClient client_id)
            transactional_id: Id for make transactional process

        Raises:
            AioKafkaProducerBadParams: raised when producer was call with bad params
            KafkaProducerError: raised when some generic error was raised form Aiokafka

        Returns:
            None
        """
        super().__init__()
        self.logger = getLogger('tonga')
        self._client = client

        # Create client_id
        if client_id is None:
            self._client_id = self._client.client_id + '-' + str(self._client.cur_instance)
        else:
            self._client_id = client_id

        self._bootstrap_servers = self._client.bootstrap_servers
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
            self.logger.exception('%s', err.__str__())
            raise AioKafkaProducerBadParams
        except KafkaError as err:
            self.logger.exception('%s', err.__str__())
            raise KafkaProducerError
        self.logger.debug('Create new producer %s', self._client_id)

    async def start_producer(self) -> None:
        """
        Start producer

        Raises:
            KafkaProducerAlreadyStartedError: raised when producer was already started
            ProducerConnectionError: raised when producer can't connect to broker
            KafkaError: raised when catch KafkaError

        Returns:
            None
        """
        if self._running:
            raise KafkaProducerAlreadyStartedError
        for retry in range(2):
            try:
                await self._kafka_producer.start()
                self._running = True
                self.logger.debug('Start producer : %s', self._client_id)
            except KafkaTimeoutError as err:
                self.logger.exception('retry: %s, err:  %s', retry, err.__str__())
                await asyncio.sleep(1)
            except ConnectionError as err:
                self.logger.exception('retry: %s, err:  %s', retry, err.__str__())
                await asyncio.sleep(1)
            except KafkaError as err:
                self.logger.exception('retry: %s, err:  %s', retry, err.__str__())
                raise err
            else:
                break
        else:
            raise ProducerConnectionError

    async def stop_producer(self) -> None:
        """
        Stop producer

        Raises:
            KafkaProducerNotStartedError: raised when producer was not started
            KafkaProducerTimeoutError: raised when producer timeout on broker
            KafkaError: raised when catch KafkaError

        Returns:
            None
        """
        if not self._running:
            raise KafkaProducerNotStartedError
        try:
            await self._kafka_producer.stop()
            self._running = False
            self.logger.debug('Stop producer : %s', self._client_id)
        except KafkaTimeoutError as err:
            self.logger.exception('%s', err.__str__())
            raise KafkaProducerTimeoutError
        except KafkaError as err:
            self.logger.exception('%s', err.__str__())
            raise err

    def is_running(self) -> bool:
        """
        Get is running

        Returns:
            bool: running
        """
        return self._running

    # Transaction sugar function
    def init_transaction(self) -> TransactionContext:
        """
        Inits transaction

        Returns:
            TransactionContext: Aiokafka TransactionContext
        """
        return self._kafka_producer.transaction()

    async def end_transaction(self, committed_offsets: Dict[str, BasePositioning], group_id: str) -> None:
        """
        Ends transaction

        Args:
            committed_offsets (Dict[str, BasePositioning]): Committed offsets during transaction
            group_id (str): Group_id to commit

        Returns:
            None
        """
        kafka_committed_offsets = dict()
        for key, positioning in committed_offsets.items():
            kafka_committed_offsets[positioning.to_topics_partition()] = positioning.get_current_offset()
        await self._kafka_producer.send_offsets_to_transaction(kafka_committed_offsets, group_id)

    async def send_and_wait(self, msg: Union[BaseRecord, StoreRecord], topic: str) -> BasePositioning:
        """
        Send a message and await an acknowledgments

        Args:
            msg (BaseRecord): Event to send in Kafka, inherit form BaseRecord
            topic (str): Topic name to send massage

        Raises:
            KeyErrorSendEvent: raised when KeyError was raised
            ValueErrorSendEvent:  raised when ValueError was raised
            TypeErrorSendEvent: raised when TypeError was raised
            KafkaError: raised when catch KafkaError
            FailToSendEvent: raised when producer fail to send event

        Returns:
            None
        """
        if not self._running:
            await self.start_producer()

        for retry in range(4):
            try:
                if isinstance(msg, BaseRecord):
                    self.logger.debug('Send record %s', msg.to_dict())
                    record_metadata = await self._kafka_producer.send_and_wait(topic=topic, value=msg,
                                                                               key=msg.partition_key)
                elif isinstance(msg, StoreRecord):
                    self.logger.debug('Send store record %s', msg.to_dict())
                    record_metadata = await self._kafka_producer.send_and_wait(topic=topic, value=msg,
                                                                               key=msg.key)
                else:
                    self.logger.error('Fail to send msg %s', msg.event_name())
                    raise UnknownEventBase
            except KafkaTimeoutError as err:
                self.logger.exception('retry: %s, err: %s', retry, err.__str__())
                await asyncio.sleep(1)
            except KeyError as err:
                self.logger.exception('retry: %s, err: %s', retry, err.__str__())
                raise KeyErrorSendEvent
            except ValueError as err:
                self.logger.exception('retry: %s, err: %s', retry, err.__str__())
                raise ValueErrorSendEvent
            except TypeError as err:
                self.logger.exception('retry: %s, err: %s', retry, err.__str__())
                raise TypeErrorSendEvent
            except KafkaError as err:
                self.logger.exception('retry: %s, err: %s', retry, err.__str__())
                raise err
            else:
                return KafkaPositioning(record_metadata.topic, record_metadata.partition, record_metadata.offset)
        else:
            raise FailToSendEvent

    async def send(self, msg: Union[BaseRecord, StoreRecord], topic: str) -> Awaitable:
        """
        Send a message and await an acknowledgments

        Args:
            msg (BaseRecord): Event to send in Kafka, inherit form BaseRecord
            topic (str): Topic name to send massage

        Raises:
            KeyErrorSendEvent: raised when KeyError was raised
            ValueErrorSendEvent:  raised when ValueError was raised
            TypeErrorSendEvent: raised when TypeError was raised
            KafkaError: raised when catch KafkaError
            FailToSendEvent: raised when producer fail to send event

        Returns:
            None
        """
        if not self._running:
            await self.start_producer()

        for retry in range(4):
            try:
                if isinstance(msg, BaseRecord):
                    self.logger.debug('Send record %s', msg.to_dict())
                    record_promise = self._kafka_producer.send(topic=topic, value=msg, key=msg.partition_key)
                elif isinstance(msg, StoreRecord):
                    self.logger.debug('Send store record %s', msg.to_dict())
                    record_promise = self._kafka_producer.send(topic=topic, value=msg, key=msg.key)
                else:
                    raise UnknownEventBase
            except KafkaTimeoutError as err:
                self.logger.exception('retry: %s, err: %s', retry, err.__str__())
                await asyncio.sleep(1)
            except KeyError as err:
                self.logger.exception('retry: %s, err: %s', retry, err.__str__())
                raise KeyErrorSendEvent
            except ValueError as err:
                self.logger.exception('retry: %s, err: %s', retry, err.__str__())
                raise ValueErrorSendEvent
            except TypeError as err:
                self.logger.exception('retry: %s, err: %s', retry, err.__str__())
                raise TypeErrorSendEvent
            except KafkaError as err:
                self.logger.exception('retry: %s, err: %s', retry, err.__str__())
                raise err
            else:
                return record_promise
        else:
            raise FailToSendEvent

    async def create_batch(self) -> BatchBuilder:
        """
        Creates an empty batch

        Returns:
            BatchBuilder: Empty batch
        """

        if not self._running:
            await self.start_producer()
        self.logger.debug('Create batch')
        return self._kafka_producer.create_batch()

    async def send_batch(self, batch: BatchBuilder, topic: str, partition: int = 0) -> None:
        """
        Sends batch

        Args:
            batch (BatchBuilder): BatchBuilder
            topic (str): Topic name
            partition (int): Partition number

        Raises:
            KeyErrorSendEvent: raised when KeyError was raised
            ValueErrorSendEvent: raised when ValueError was raised
            TypeErrorSendEvent: raised when TypeError was raised
            KafkaError: raised when catch KafkaError
            FailToSendBatch: raised when producer fail to send batch

        Returns:
            None
        """

        if not self._running:
            await self.start_producer()

        for retry in range(4):
            try:
                self.logger.debug('Send batch')
                await self._kafka_producer.send_batch(batch=batch, topic=topic, partition=partition)
            except KafkaTimeoutError as err:
                self.logger.exception('retry: %s, err: %s', retry, err.__str__())
                await asyncio.sleep(1)
            except KeyError as err:
                self.logger.exception('retry: %s, err: %s', retry, err.__str__())
                raise KeyErrorSendEvent
            except ValueError as err:
                self.logger.exception('retry: %s, err: %s', retry, err.__str__())
                raise ValueErrorSendEvent
            except TypeError as err:
                self.logger.exception('retry: %s, err: %s', retry, err.__str__())
                raise TypeErrorSendEvent
            except KafkaError as err:
                self.logger.exception('retry: %s, err: %s', retry, err.__str__())
                raise err
            else:
                break
        else:
            raise FailToSendBatch

    async def partitions_by_topic(self, topic: str) -> List[int]:
        """
        Get partitions by topic name

        Args:
            topic (str): topic name

        Returns:
            List[int]: list of partitions
        """

        if not self._running:
            await self.start_producer()
        try:
            self.logger.debug('Get partitions by topic')
            partitions = await self._kafka_producer.partitions_for(topic)
        except KafkaTimeoutError as err:
            self.logger.exception('%s', err.__str__())
            raise KafkaProducerTimeoutError
        except KafkaError as err:
            self.logger.exception('%s', err.__str__())
            raise err
        return partitions

    def get_producer(self) -> AIOKafkaProducer:
        """
        Get kafka producer

        Returns:
            AIOKafkaProducer: AioKafkaProducer instance
        """
        return self._kafka_producer
